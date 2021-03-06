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

package org.opengroup.osdu.storage.provider.azure.repository;

import org.opengroup.osdu.azure.multitenancy.TenantInfoDoc;
import org.opengroup.osdu.core.common.cache.ICache;
import org.opengroup.osdu.core.common.model.http.DpsHeaders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public class GroupsInfoRepository extends SimpleCosmosStoreRepository<TenantInfoDoc> {

    @Autowired
    private DpsHeaders headers;

    @Autowired
    private String tenantInfoCollection;

    @Autowired
    private String cosmosDBName;

    public GroupsInfoRepository() {
        super(TenantInfoDoc.class);
    }

    @Autowired
    ICache<String, TenantInfoDoc> tenantInfoDocCache;

    public Optional<TenantInfoDoc> findById(@NonNull String id) {
        TenantInfoDoc tenantInfoDoc = this.tenantInfoDocCache.get(id);

        if (tenantInfoDoc != null) {
            return Optional.of(tenantInfoDoc);
        }
        else {
            Optional<TenantInfoDoc> tenantInfoDocOptional = this.findById(id, headers.getPartitionId(), cosmosDBName, tenantInfoCollection, id);
            tenantInfoDocOptional.ifPresent(infoDoc -> this.tenantInfoDocCache.put(id, infoDoc));
            return tenantInfoDocOptional;
        }
    }
}