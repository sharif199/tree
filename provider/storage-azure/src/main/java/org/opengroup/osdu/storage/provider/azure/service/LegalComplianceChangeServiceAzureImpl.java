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

package org.opengroup.osdu.storage.provider.azure.service;

import org.opengroup.osdu.core.common.model.http.DpsHeaders;
import org.opengroup.osdu.core.common.model.indexer.OperationType;
import org.opengroup.osdu.core.common.model.legal.LegalCompliance;
import org.opengroup.osdu.core.common.model.legal.jobs.*;
import org.opengroup.osdu.core.common.model.storage.PubSubInfo;
import org.opengroup.osdu.core.common.model.storage.RecordMetadata;
import org.opengroup.osdu.core.common.model.storage.RecordState;
import org.opengroup.osdu.storage.logging.StorageAuditLogger;
import org.opengroup.osdu.storage.provider.azure.MessageBusImpl;
import org.opengroup.osdu.storage.provider.azure.cache.LegalTagCache;
import org.opengroup.osdu.storage.provider.interfaces.IRecordsMetadataRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static java.util.Collections.singletonList;

@Component
public class LegalComplianceChangeServiceAzureImpl implements ILegalComplianceChangeService {
    private final static Logger LOGGER = LoggerFactory.getLogger(LegalComplianceChangeServiceAzureImpl.class);
    @Autowired
    private IRecordsMetadataRepository recordsRepo;
    @Autowired
    private DpsHeaders headers;
    @Autowired
    private LegalTagCache legalTagCache;
    @Autowired
    private MessageBusImpl pubSubclient;

    @Override
    public Map<String, LegalCompliance> updateComplianceOnRecords(LegalTagChangedCollection legalTagsChanged,
                                                                  DpsHeaders headers) throws ComplianceUpdateStoppedException {
        Map<String, LegalCompliance> output = new HashMap<>();

        for (LegalTagChanged lt : legalTagsChanged.getStatusChangedTags()) {
            ComplianceChangeInfo complianceChangeInfo = this.getComplianceChangeInfo(lt);
            if (complianceChangeInfo == null) {
                continue;
            }
            String cursor = null;
            do {
                //TODO replace with the new method queryByLegal
                AbstractMap.SimpleEntry<String, List<RecordMetadata>> results = recordsRepo
                        .queryByLegalTagName(lt.getChangedTagName(), 500, cursor);
                cursor = results.getKey();
                if (results.getValue() != null && !results.getValue().isEmpty()) {
                    List<RecordMetadata> recordsMetadata = results.getValue();
                    PubSubInfo[] pubsubInfos = this.updateComplianceStatus(complianceChangeInfo, recordsMetadata, output);
                    this.recordsRepo.createOrUpdate(recordsMetadata);
                    StringBuilder recordsId = new StringBuilder();
                    for (RecordMetadata recordMetadata : recordsMetadata) {
                        recordsId.append(", ").append(recordMetadata.getId());
                    }
                    LOGGER.debug("Record Updated Successfully {}",recordsId.substring(2));

                    this.pubSubclient.publishMessage(headers, pubsubInfos);
                }
            } while (cursor != null);
        }
        return output;
    }

    private PubSubInfo[] updateComplianceStatus(ComplianceChangeInfo complianceChangeInfo,
                                                List<RecordMetadata> recordMetadata, Map<String, LegalCompliance> output) {

        PubSubInfo[] pubsubInfo = new PubSubInfo[recordMetadata.size()];

        int i = 0;
        for (RecordMetadata rm : recordMetadata) {
            rm.getLegal().setStatus(complianceChangeInfo.getNewState());
            rm.setStatus(complianceChangeInfo.getNewRecordState());
            pubsubInfo[i] = new PubSubInfo(rm.getId(), rm.getKind(), complianceChangeInfo.getPubSubEvent(), rm.getKind());
            output.put(rm.getId(), complianceChangeInfo.getNewState());
            i++;
        }

        return pubsubInfo;
    }

    private ComplianceChangeInfo getComplianceChangeInfo(LegalTagChanged lt) {
        ComplianceChangeInfo output = null;

        if (lt.getChangedTagStatus().equalsIgnoreCase("compliant")) {
            output = new ComplianceChangeInfo(LegalCompliance.compliant, OperationType.create, RecordState.active);
        } else if (lt.getChangedTagStatus().equalsIgnoreCase("incompliant")) {
            this.legalTagCache.delete(lt.getChangedTagName());
            output = new ComplianceChangeInfo(LegalCompliance.incompliant, OperationType.delete, RecordState.deleted);
        } else {
            this.LOGGER.warn(String.format("Unknown LegalTag compliance status received %s %s",
                    lt.getChangedTagStatus(), lt.getChangedTagName()));
        }
        return output;
    }

}
