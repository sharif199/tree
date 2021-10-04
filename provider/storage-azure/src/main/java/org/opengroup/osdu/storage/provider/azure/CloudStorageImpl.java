// Copyright © Microsoft Corporation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.opengroup.osdu.storage.provider.azure;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.http.HttpStatus;
import org.opengroup.osdu.azure.blobstorage.BlobStore;
import org.opengroup.osdu.core.common.logging.JaxRsDpsLog;
import org.opengroup.osdu.core.common.model.entitlements.Acl;
import org.opengroup.osdu.core.common.model.http.AppException;
import org.opengroup.osdu.core.common.model.http.DpsHeaders;
import org.opengroup.osdu.core.common.model.storage.*;
import org.opengroup.osdu.core.common.util.Crc32c;
import org.opengroup.osdu.storage.provider.azure.repository.GroupsInfoRepository;
import org.opengroup.osdu.storage.provider.azure.util.RecordUtil;
import org.opengroup.osdu.storage.provider.interfaces.ICloudStorage;
import org.opengroup.osdu.storage.provider.interfaces.IRecordsMetadataRepository;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import javax.inject.Named;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static org.apache.commons.codec.binary.Base64.encodeBase64;

@Repository
public class CloudStorageImpl implements ICloudStorage {
    @Autowired
    private JaxRsDpsLog logger;

    @Autowired
    private EntitlementsAndCacheServiceAzure dataEntitlementsService;

    @Autowired
    private DpsHeaders headers;

    @Autowired
    private ExecutorService threadPool;

    @Autowired
    private IRecordsMetadataRepository recordRepository;

    @Autowired
    private BlobStore blobStore;

    @Autowired
    private GroupsInfoRepository groupsInfoRepository;

    @Autowired
    private RecordUtil recordUtil;

    @Autowired
    @Named("STORAGE_CONTAINER_NAME")
    private String containerName;

    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void write(RecordProcessing... recordsProcessing) {
        List<Callable<Boolean>> tasks = new ArrayList<>();
        String dataPartitionId = headers.getPartitionId();
        for (RecordProcessing rp : recordsProcessing) {
            tasks.add(() -> this.writeBlobThread(rp, dataPartitionId));
        }

        try {
            for (Future<Boolean> result : this.threadPool.invokeAll(tasks)) {
                result.get();
            }
            MDC.put("record-count",String.valueOf(tasks.size()));
        } catch (InterruptedException | ExecutionException e) {
            throw new AppException(HttpStatus.SC_INTERNAL_SERVER_ERROR, "Error during record ingestion",
                    "An unexpected error on writing the record has occurred", e);
        }
    }

    @Override
    public Map<String, Acl> updateObjectMetadata(List<RecordMetadata> recordsMetadata, List<String> recordsId, List<RecordMetadata> validMetadata, List<String> lockedRecords, Map<String, String> recordsIdMap) {

        Map<String, org.opengroup.osdu.core.common.model.entitlements.Acl> originalAcls = new HashMap<>();
        Map<String, RecordMetadata> currentRecords = this.recordRepository.get(recordsId);

        for (RecordMetadata recordMetadata : recordsMetadata) {
            String id = recordMetadata.getId();
            String idWithVersion = recordsIdMap.get(id);
            // validate that updated metadata has the same version
            if (!id.equalsIgnoreCase(idWithVersion)) {
                long previousVersion = Long.parseLong(idWithVersion.split(":")[3]);
                long currentVersion = currentRecords.get(id).getLatestVersion();
                // if version is different, do not update
                if (previousVersion != currentVersion) {
                    lockedRecords.add(idWithVersion);
                    continue;
                }
            }
            validMetadata.add(recordMetadata);
            originalAcls.put(recordMetadata.getId(), currentRecords.get(id).getAcl());
        }
        return originalAcls;
    }

    @Override
    public void revertObjectMetadata(List<RecordMetadata> recordsMetadata, Map<String, org.opengroup.osdu.core.common.model.entitlements.Acl> originalAcls) {
        List<RecordMetadata> originalAclRecords = new ArrayList<>();
        for (RecordMetadata recordMetadata : recordsMetadata) {
            Acl acl = originalAcls.get(recordMetadata.getId());
            recordMetadata.setAcl(acl);
            originalAclRecords.add(recordMetadata);
        }
        try {
            this.recordRepository.createOrUpdate(originalAclRecords);
        } catch (Exception e) {
            throw new AppException(HttpStatus.SC_INTERNAL_SERVER_ERROR, "Error while reverting metadata: in revertObjectMetadata.","Internal server error.", e);
        }
    }

    private boolean writeBlobThread(RecordProcessing rp, String dataPartitionId)
    {
        Gson gson = new GsonBuilder().serializeNulls().create();
        RecordMetadata rmd = rp.getRecordMetadata();
        String path = buildPath(rmd);
        String content = gson.toJson(rp.getRecordData());
        blobStore.writeToStorageContainer(dataPartitionId, path, content, containerName);
        return true;
    }

    @Override
    public Map<String, String> getHash(Collection<RecordMetadata> records) {
        Map<String, String> hashes = new HashMap<>();
        RecordData data;
        for (RecordMetadata rm : records) {
            String jsonData = this.read(rm, rm.getLatestVersion(), false);
            try {
                data = objectMapper.readValue(jsonData, RecordData.class);
            } catch (JsonProcessingException e){
                logger.error(String.format("Error while converting metadata for record %s", rm.getId()), e);
                continue;
            }
            String hash = getHash(data);
            hashes.put(rm.getId(), hash);
        }
        return hashes;
    }

    @Override
    public boolean isDuplicateRecord(TransferInfo transfer, Map<String, String> hashMap, Map.Entry<RecordMetadata, RecordData> kv) {
        RecordMetadata updatedRecordMetadata = kv.getKey();
        RecordData recordData = kv.getValue();
        String recordHash = hashMap.get(updatedRecordMetadata.getId());

        String newHash = getHash(recordData);

        if (newHash.equals(recordHash)) {
            transfer.getSkippedRecords().add(updatedRecordMetadata.getId());
            return true;
        }else{
            return false;
        }
    }

    private String getHash(RecordData data) {
        Gson gson = new Gson();
        Crc32c checksumGenerator = new Crc32c();
        String newRecordStr = gson.toJson(data);
        byte[] bytes = newRecordStr.getBytes(StandardCharsets.UTF_8);
        checksumGenerator.update(bytes, 0, bytes.length);
        bytes = checksumGenerator.getValueAsBytes();
        String newHash = new String(encodeBase64(bytes));
        return newHash;
    }

    @Override
    public void delete(RecordMetadata record) {
        if (!record.hasVersion()) {
            this.logger.warning(String.format("Record %s does not have versions available", record.getId()));
            return;
        }

        validateOwnerAccessToRecord(record);
        String path = this.buildPath(record);
        blobStore.deleteFromStorageContainer(headers.getPartitionId(), path, containerName);
    }

    @Override
    public void deleteVersion(RecordMetadata record, Long version) {
        validateOwnerAccessToRecord(record);
        String path = this.buildPath(record, version.toString());
        blobStore.deleteFromStorageContainer(headers.getPartitionId(), path, containerName);
    }

    @Override
    public boolean hasAccess(RecordMetadata... records) {
        if (ArrayUtils.isEmpty(records)) {
            return true;
        }

        boolean hasAtLeastOneActiveRecord = false;
        for (RecordMetadata record : records) {
            if (!record.getStatus().equals(RecordState.active)) {
                continue;
            }

            if (!record.hasVersion()) {
                this.logger.warning(String.format("Record %s does not have versions available", record.getId()));
                continue;
            }

            hasAtLeastOneActiveRecord = true;
            if (hasViewerAccessToRecord(record))
                return true;
        }

        return !hasAtLeastOneActiveRecord;
    }

    private boolean hasViewerAccessToRecord(RecordMetadata record)
    {
        boolean isEntitledForViewing = dataEntitlementsService.hasAccessToData(headers,
                new HashSet<>(Arrays.asList(record.getAcl().getViewers())));
        boolean isRecordOwner = record.getUser().equalsIgnoreCase(headers.getUserEmail());
        return isEntitledForViewing || isRecordOwner;
    }

    private boolean hasOwnerAccessToRecord(RecordMetadata record)
    {
        return dataEntitlementsService.hasAccessToData(headers,
                new HashSet<>(Arrays.asList(record.getAcl().getOwners())));
    }

    private void validateOwnerAccessToRecord(RecordMetadata record)
    {
        if (!hasOwnerAccessToRecord(record)) {
            logger.warning(String.format("%s has no owner access to %s", headers.getUserEmail(), record.getId()));
            throw new AppException(HttpStatus.SC_FORBIDDEN, ACCESS_DENIED_ERROR_REASON, ACCESS_DENIED_ERROR_MSG);
        }
    }

    private void validateViewerAccessToRecord(RecordMetadata record)
    {
        if (!hasViewerAccessToRecord(record)) {
            logger.warning(String.format("%s has no viewer access to %s", headers.getUserEmail(), record.getId()));
            throw new AppException(HttpStatus.SC_FORBIDDEN,  ACCESS_DENIED_ERROR_REASON, ACCESS_DENIED_ERROR_MSG);
        }
    }

    @Override
    public String read(RecordMetadata record, Long version, boolean checkDataInconsistency) {
        validateViewerAccessToRecord(record);
        String path = this.buildPath(record, version.toString());
        return blobStore.readFromStorageContainer(headers.getPartitionId(), path, containerName);
    }

    @Override
    public Map<String, String> read(Map<String, String> objects) {
        List<Callable<Boolean>> tasks = new ArrayList<>();
        Map<String, String> map = new HashMap<>();

        List<String> recordIds = new ArrayList<>(objects.keySet());
        Map<String, RecordMetadata> recordsMetadata = this.recordRepository.get(recordIds);

        String dataPartitionId = headers.getPartitionId();

        for (String recordId : recordIds) {
            RecordMetadata recordMetadata = recordsMetadata.get(recordId);
            if (!hasViewerAccessToRecord(recordMetadata)) {
                map.put(recordId, null);
                continue;
            }
            String path = objects.get(recordId);
            tasks.add(() -> this.readBlobThread(recordId, path, map, dataPartitionId));
        }

        try {
            for (Future<Boolean> result : this.threadPool.invokeAll(tasks)) {
                result.get();
            }
        } catch (InterruptedException | ExecutionException e) {
            String errorMessage = "Unable to process parallel blob download";
            logger.error(errorMessage, e);
            throw new AppException(500, errorMessage, e.getMessage());
        }

        return map;
    }

    private boolean readBlobThread(String key, String path, Map<String, String> map, String dataPartitionId) {
        String content = blobStore.readFromStorageContainer(dataPartitionId, path, containerName);
        map.put(key, content);
        return true;
    }

    private String buildPath(RecordMetadata record)
    {
        String path = record.getKind() + "/" + record.getId() + "/" + record.getLatestVersion();
        return path;
    }

    private String buildPath(RecordMetadata record, String version) {
        String kind = recordUtil.getKindForVersion(record, version);
        String path = kind + "/" + record.getId() + "/" + version;
        return path;
    }
}