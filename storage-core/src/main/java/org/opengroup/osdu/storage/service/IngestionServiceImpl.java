// Copyright 2017-2019, Schlumberger
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

package org.opengroup.osdu.storage.service;

import com.google.common.base.Strings;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.http.HttpStatus;
import org.opengroup.osdu.core.common.entitlements.IEntitlementsAndCacheService;
import org.opengroup.osdu.core.common.model.http.DpsHeaders;
import org.opengroup.osdu.core.common.model.legal.Legal;
import org.opengroup.osdu.core.common.model.legal.LegalCompliance;
import org.opengroup.osdu.core.common.model.indexer.OperationType;
import org.opengroup.osdu.core.common.model.storage.*;
import org.opengroup.osdu.core.common.model.http.AppException;
import org.opengroup.osdu.core.common.legal.ILegalService;
import org.opengroup.osdu.core.common.logging.JaxRsDpsLog;
import org.opengroup.osdu.core.common.model.storage.validation.ValidationDoc;
import org.opengroup.osdu.core.common.model.tenant.TenantInfo;
import org.opengroup.osdu.core.common.storage.*;
import org.opengroup.osdu.storage.logging.StorageAuditLogger;
import org.opengroup.osdu.storage.policy.service.IPolicyService;
import org.opengroup.osdu.storage.provider.interfaces.ICloudStorage;
import org.opengroup.osdu.storage.provider.interfaces.IRecordsMetadataRepository;
import org.opengroup.osdu.storage.util.api.RecordUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.Map.Entry;

import io.jsonwebtoken.lang.Collections;

@Service
public class IngestionServiceImpl implements IngestionService {

	@Autowired
	private IRecordsMetadataRepository recordRepository;

	@Autowired
	private ICloudStorage cloudStorage;

	@Autowired
	private IPersistenceService persistenceService;

	@Autowired
	private ILegalService legalService;

	@Autowired
	private StorageAuditLogger auditLogger;

	@Autowired
	private DpsHeaders headers;

	@Autowired
	private TenantInfo tenant;

	@Autowired
    private JaxRsDpsLog logger;

	@Autowired
	private IEntitlementsAndCacheService entitlementsAndCacheService;

	@Autowired
	private DataAuthorizationService dataAuthorizationService;

	@Autowired(required = false)
	private IPolicyService policyService;

	@Autowired
	private RecordUtil recordUtil;

	@Override
	public TransferInfo createUpdateRecords(boolean skipDupes, List<Record> inputRecords, String user) {
		this.validateKindFormat(inputRecords);
		this.validateRecordIds(inputRecords);
		this.validateAcl(inputRecords);

		TransferInfo transfer = new TransferInfo(user, inputRecords.size());

		List<RecordProcessing> recordsToProcess = this.getRecordsForProcessing(skipDupes, inputRecords, transfer);

		this.sendRecordsForProcessing(recordsToProcess, transfer);
		return transfer;
	}

	private void validateAcl(List<Record> inputRecords) {
		Set<String> acls = new HashSet<>();
		for (Record record : inputRecords) {
			String[] viewers = record.getAcl().getViewers();
			String[] owners = record.getAcl().getOwners();
			for (String viewer : viewers) {
				acls.add(viewer);
			}
			for (String owner : owners) {
				acls.add(owner);
			}
		}
		if (!this.entitlementsAndCacheService.isValidAcl(this.headers, acls)) {
			throw new AppException(HttpStatus.SC_BAD_REQUEST, "Invalid ACL", "Acl not match with tenant or domain");
		}
	}

	private void validateKindFormat(List<Record> inputRecords) {
		for (Record record : inputRecords) {
			if (!record.getKind().matches(ValidationDoc.KIND_REGEX)) {
				String msg = String.format(
						"Invalid kind: '%s', does not follow the required naming convention",
						record.getKind());

				throw new AppException(HttpStatus.SC_BAD_REQUEST, "Invalid kind", msg);
			}
		}
	}

	private void validateRecordIds(List<Record> inputRecords) {
		String tenantName = tenant.getName();

		Set<String> ids = new HashSet<>();
		for (Record record : inputRecords) {
			String id = record.getId();

			if (Strings.isNullOrEmpty(record.getKind())) {
				throw new AppException(HttpStatus.SC_BAD_REQUEST, "Bad request",
							"Must have valid kind");
			}

			if (!Strings.isNullOrEmpty(id)) {
				if (ids.contains(id)) {
					throw new AppException(HttpStatus.SC_BAD_REQUEST, "Bad request",
							"Cannot update the same record multiple times in the same request. Id: " + id);
				}

				if (!Record.isRecordIdValid(id, tenantName, record.getKind())) {
					String kindSubType = record.getKind().split(":")[2];
					String msg = String.format(
							"The record '%s' does not follow the naming convention: The record id must be in the format of <tenantId>:<kindSubType>:<uniqueId>. Example: %s:%s:<uuid>",
							id, tenantName, kindSubType);
					throw new AppException(HttpStatus.SC_BAD_REQUEST, "Invalid record id", msg);
				}

				ids.add(id);
			} else {
				record.createNewRecordId(tenantName, record.getKind());
			}
		}
	}

	private List<RecordProcessing> getRecordsForProcessing(boolean skipDupes, List<Record> inputRecords,
			TransferInfo transfer) {
		Map<String, List<RecordIdWithVersion>> recordParentMap = new HashMap<>();
		List<RecordProcessing> recordsToProcess = new ArrayList<>();

		List<String> ids = this.getRecordIds(inputRecords, recordParentMap);
		Map<String, RecordMetadata> existingRecords = this.recordRepository.get(ids);

		this.validateParentsExist(existingRecords, recordParentMap);
		if(this.dataAuthorizationService.policyEnabled()) {
		    this.validateUserAccessAndCompliancePolicyConstraints(inputRecords, existingRecords, recordParentMap);
		} else {
			this.validateUserAccessAndComplianceConstraints(inputRecords, existingRecords, recordParentMap);
		}

		Map<RecordMetadata, RecordData> recordUpdatesMap = new HashMap<>();
        Map<RecordMetadata, RecordData> recordUpdateWithoutVersions = new HashMap<>();

		final long currentTimestamp = System.currentTimeMillis();

		inputRecords.forEach(record -> {
			RecordData recordData = new RecordData(record);

			if (!existingRecords.containsKey(record.getId())) {
				RecordMetadata recordMetadata = new RecordMetadata(record);
				recordMetadata.setUser(transfer.getUser());
				recordMetadata.setStatus(RecordState.active);
				recordMetadata.setCreateTime(currentTimestamp);
				recordMetadata.addGcsPath(transfer.getVersion());

				recordsToProcess.add(new RecordProcessing(recordData, recordMetadata, OperationType.create, recordMetadata.getKind()));
			} else {
				RecordMetadata existingRecordMetadata = existingRecords.get(record.getId());
				RecordMetadata updatedRecordMetadata = new RecordMetadata(record);

				List<String> versions = new ArrayList<>();
				versions.addAll(existingRecordMetadata.getGcsVersionPaths());

				updatedRecordMetadata.setUser(existingRecordMetadata.getUser());
				updatedRecordMetadata.setCreateTime(existingRecordMetadata.getCreateTime());
				updatedRecordMetadata.setGcsVersionPaths(versions);

                if (versions.isEmpty()) {
                    this.logger.warning(String.format("Record %s does not have versions available", updatedRecordMetadata.getId()));
                    recordUpdateWithoutVersions.put(updatedRecordMetadata, recordData);
                } else {
                    recordUpdatesMap.put(updatedRecordMetadata, recordData);
                }
				if (skipDupes && recordUpdatesMap.size() > 0) {
					this.removeDuplicatedRecords(recordUpdatesMap, transfer);
				}
				recordUpdatesMap.putAll(recordUpdateWithoutVersions);
				this.populateUpdatedRecords(recordUpdatesMap, recordsToProcess, transfer, currentTimestamp, existingRecordMetadata.getKind());
			}
		});

		return recordsToProcess;
	}

	private void validateUserAccessAndComplianceConstraints(
			List<Record> inputRecords, Map<String, RecordMetadata> existingRecords,  Map<String, List<RecordIdWithVersion>> recordParentMap) {
		this.validateUserHasAccessToAllRecords(existingRecords);
		this.validateLegalConstraints(inputRecords);
		this.validateOwnerAccessOnExistingRecords(inputRecords, existingRecords);
		this.populateLegalInfoFromParents(inputRecords, existingRecords, recordParentMap);
	}

	private void validateOwnerAccessOnExistingRecords(List<Record> inputRecords, Map<String, RecordMetadata> existingRecords) {
		for (Record record: inputRecords) {
			if (!existingRecords.containsKey(record.getId())) {
				continue;
			}
			RecordMetadata existingRecordMetadata = existingRecords.get(record.getId());
			if(!this.entitlementsAndCacheService.hasOwnerAccess(headers, existingRecordMetadata.getAcl().getOwners())) {
				this.logger.warning(String.format("User does not have owner access to record %s", record.getId()));
				throw new AppException(HttpStatus.SC_FORBIDDEN, "User Unauthorized", "User is not authorized to update records.");
			}
		}
	}

	private void validateParentsExist(Map<String, RecordMetadata> existingRecords,
			Map<String, List<RecordIdWithVersion>> recordParentMap) {

		for (Entry<String, List<RecordIdWithVersion>> entry : recordParentMap.entrySet()) {
			List<RecordIdWithVersion> parentRecordIdsWithVersions = entry.getValue();
			for (RecordIdWithVersion parentRecordIdWithVersion : parentRecordIdsWithVersions) {
				String parentId = parentRecordIdWithVersion .getRecordId();
				if (!existingRecords.containsKey(parentId)) {
					throw new AppException(HttpStatus.SC_NOT_FOUND, "Record not found",
							String.format("The record '%s' was not found", parentRecordIdWithVersion ));
				}
				RecordMetadata parentRecordMetadata  = existingRecords.get(parentId);
				long version = parentRecordIdWithVersion .getRecordVersion();
				if (!recordUtil.hasVersionPath(parentRecordMetadata.getGcsVersionPaths(), version)) {
					throw new AppException(HttpStatus.SC_NOT_FOUND, "RecordMetadata version not found",
							String.format("The RecordMetadata version %d for parent record '%s' was not found", version, parentId));
				}
			}
		}
	}

	private void validateLegalConstraints(List<Record> inputRecords) {

		Set<String> legalTags = this.getLegalTags(inputRecords);
		Set<String> ordc = this.getORDC(inputRecords);

		this.legalService.validateLegalTags(legalTags);
		this.legalService.validateOtherRelevantDataCountries(ordc);
	}

	private void populateLegalInfoFromParents(List<Record> inputRecords,
										  Map<String, RecordMetadata> existingRecordsMetadata,
										  Map<String, List<RecordIdWithVersion>> recordParentMap) {

		this.legalService.populateLegalInfoFromParents(inputRecords, existingRecordsMetadata, recordParentMap);

		for (Record record : inputRecords) {
			Legal legal = record.getLegal();
			legal.setStatus(LegalCompliance.compliant);
		}
	}

	private void validateUserHasAccessToAllRecords(Map<String, RecordMetadata> existingRecords) {
		RecordMetadata[] records = existingRecords.values().toArray(new RecordMetadata[existingRecords.size()]);
		if (!this.cloudStorage.hasAccess(records)) {
			throw new AppException(HttpStatus.SC_FORBIDDEN, "Access denied",
					"The user is not authorized to perform this action");
		}
	}

	private void removeDuplicatedRecords(Map<RecordMetadata, RecordData> recordUpdatesMap, TransferInfo transfer) {
		Collection<RecordMetadata> metadataList = recordUpdatesMap.keySet();
		Map<String, String> hashMap = this.cloudStorage.getHash(metadataList);
		recordUpdatesMap
				.entrySet()
				.removeIf(kv -> this.cloudStorage.isDuplicateRecord(transfer, hashMap, kv));
	}

	private void populateUpdatedRecords(Map<RecordMetadata, RecordData> recordUpdatesMap,
			List<RecordProcessing> recordsToProcess, TransferInfo transfer, long timestamp, String originalKind) {
		for (Map.Entry<RecordMetadata, RecordData> recordEntry : recordUpdatesMap.entrySet()) {
			RecordMetadata recordMetadata = recordEntry.getKey();
			recordMetadata.addGcsPath(transfer.getVersion());
			recordMetadata.setModifyUser(transfer.getUser());
			recordMetadata.setModifyTime(timestamp);
			recordMetadata.setStatus(RecordState.active);

			RecordData recordData = recordEntry.getValue();

			recordsToProcess.add(new RecordProcessing(recordData, recordMetadata, OperationType.update, originalKind));
		}
	}

	private void sendRecordsForProcessing(List<RecordProcessing> records, TransferInfo transferInfo) {
		if (!records.isEmpty()) {
			this.persistenceService.persistRecordBatch(new TransferBatch(transferInfo, records));
			this.auditLogger.createOrUpdateRecordsSuccess(this.extractRecordIds(records));
		}
	}

	private List<String> extractRecordIds(List<RecordProcessing> records) {
		List<String> recordIds = new ArrayList<>();
		for (RecordProcessing processing : records) {
			recordIds.add(processing.getRecordMetadata().getId());
		}
		return recordIds;
	}

	private List<String> getRecordIds(List<Record> records, Map<String, List<RecordIdWithVersion>> recordParentMap) {
		List<String> ids = new ArrayList<>();
		for (Record record : records) {
			if (record.getAncestry() != null && !Collections.isEmpty(record.getAncestry().getParents())) {

				List<RecordIdWithVersion> parents = new ArrayList<>();

				for (String parent : record.getAncestry().getParents()) {
					String[] tokens = parent.split(":");
					String parentRecordId = String.join(":", tokens[0], tokens[1], tokens[2]);
					Long parentRecordVersion = Long.parseLong(tokens[3]);

					parents.add(
							RecordIdWithVersion
									.builder()
									.recordId(parentRecordId)
									.recordVersion(parentRecordVersion)
									.build()
					);
					ids.add(parentRecordId);
				}

				recordParentMap.put(record.getId(), parents);
			}

			ids.add(record.getId());
		}

		return ids;
	}

	private Set<String> getLegalTags(List<Record> inputRecords) {
		Set<String> legalTags = new HashSet<>();

		for (Record record : inputRecords) {
			if (record.getLegal().hasLegaltags()) {
				legalTags.addAll(record.getLegal().getLegaltags());
			}
		}

		return legalTags;
	}

	private Set<String> getORDC(List<Record> inputRecords) {
		Set<String> ordc = new HashSet<>();

		for (Record record : inputRecords) {
			if (record.getLegal().getOtherRelevantDataCountries() != null
					&& !record.getLegal().getOtherRelevantDataCountries().isEmpty()) {
				ordc.addAll(record.getLegal().getOtherRelevantDataCountries());
			}
		}

		return ordc;
	}

	private void validateUserAccessAndCompliancePolicyConstraints(
			List<Record> inputRecords, Map<String, RecordMetadata> existingRecords,  Map<String, List<RecordIdWithVersion>> recordParentMap) {
		this.populateLegalInfoFromParents(inputRecords, existingRecords, recordParentMap);
		for (Record record : inputRecords) {
			RecordMetadata recordMetadata;
			OperationType operationType;
			if (existingRecords.containsKey(record.getId())) {
				recordMetadata = existingRecords.get(record.getId());
				operationType = OperationType.update;
			} else {
				recordMetadata = new RecordMetadata(record);
				operationType = OperationType.create;
			}
			if (!this.policyService.evaluateStorageDataAuthorizationPolicy(recordMetadata, operationType)) {
				throw new AppException(HttpStatus.SC_FORBIDDEN,
						"User Unauthorized", "User is not authorized to create or update records.", String.format("User does not have required access to record %s", record.getId()));
			}
		}
	}
}