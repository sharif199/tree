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

import com.azure.cosmos.CosmosException;
import com.google.common.base.Strings;
import com.lambdaworks.redis.RedisException;
import org.apache.http.HttpStatus;
import org.opengroup.osdu.azure.query.CosmosStorePageRequest;
import org.opengroup.osdu.core.common.cache.ICache;
import org.opengroup.osdu.core.common.logging.JaxRsDpsLog;
import org.opengroup.osdu.core.common.model.http.AppException;
import org.opengroup.osdu.core.common.model.storage.DatastoreQueryResult;
import org.opengroup.osdu.core.common.model.storage.RecordState;
import org.opengroup.osdu.core.common.util.Crc32c;
import org.opengroup.osdu.storage.provider.azure.RecordMetadataDoc;
import org.opengroup.osdu.storage.provider.azure.SchemaDoc;
import org.opengroup.osdu.storage.provider.interfaces.IQueryRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Repository;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.List;

@Repository
public class QueryRepository implements IQueryRepository {

    @Autowired
    private RecordMetadataRepository record;

    @Autowired
    private SchemaRepository schema;

    @Autowired
    private JaxRsDpsLog logger;

    @Autowired
    @Qualifier("CursorCache")
    private ICache<String, String> cursorCache;

    @Override
    public DatastoreQueryResult getAllKinds(Integer limit, String cursor) {

        boolean paginated  = false;

        int numRecords = PAGE_SIZE;
        if (limit != null) {
            numRecords = limit > 0 ? limit : PAGE_SIZE;
            paginated = true;
        }

        if (cursor != null && !cursor.isEmpty()) {
            paginated = true;
        }

        Sort sort = Sort.by(Sort.Direction.ASC, "kind");
        DatastoreQueryResult dqr = new DatastoreQueryResult();
        List<String> kinds = new ArrayList();
        Iterable<SchemaDoc> docs;

        try {
            if (paginated) {
                final Page<SchemaDoc> docPage = schema.findAll(CosmosStorePageRequest.of(0, numRecords, cursor, sort));
                Pageable pageable = docPage.getPageable();
                String continuation = null;
                if (pageable instanceof CosmosStorePageRequest) {
                    continuation = ((CosmosStorePageRequest) pageable).getRequestContinuation();
                }
                dqr.setCursor(continuation);
                docs = docPage.getContent();
            } else {
                docs = schema.findAll(sort);
            }
            docs.forEach(
                    d -> kinds.add(d.getKind()));
            dqr.setResults(kinds);
        } catch (CosmosException e) {
            if (e.getStatusCode() == HttpStatus.SC_BAD_REQUEST && e.getMessage().contains("INVALID JSON in continuation token"))
                throw this.getInvalidCursorException();
            else
                throw e;
        } catch (Exception e) {
            throw e;
        }

        return dqr;

    }

    @Override
    public DatastoreQueryResult getAllRecordIdsFromKind(String kind, Integer limit, String hashedCursorKey) {
        Assert.notNull(kind, "kind must not be null");

        boolean paginated = false;
        int numRecords = PAGE_SIZE;
        if (limit != null) {
            numRecords = limit > 0 ? limit : PAGE_SIZE;
            paginated = true;
        }

        String cursor = null;
        if (hashedCursorKey != null && !hashedCursorKey.isEmpty()) {
            paginated = true;
            try {
                cursor = this.cursorCache.get(hashedCursorKey);
            } catch (RedisException ex) {
                this.logger.error(String.format("Error getting key %s from redis: %s", hashedCursorKey, ex.getMessage()), ex);
            }

            if (Strings.isNullOrEmpty(cursor)) throw this.getInvalidCursorException();
        }
        String status = RecordState.active.toString();
        DatastoreQueryResult dqr = new DatastoreQueryResult();
        List<String> ids = new ArrayList<>();
        Iterable<RecordMetadataDoc> docs;

        try {
            if (paginated) {
                final Page<RecordMetadataDoc> docPage = record.findIdsByMetadata_kindAndMetadata_status(kind, status,
                        CosmosStorePageRequest.of(0, numRecords, cursor));
                Pageable pageable = docPage.getPageable();
                String continuation = null;
                if (pageable instanceof CosmosStorePageRequest) {
                    continuation = ((CosmosStorePageRequest) pageable).getRequestContinuation();
                }

                if (!Strings.isNullOrEmpty(continuation)) {
                    String hashedCursor = Crc32c.hashToBase64EncodedString(continuation);
                    this.cursorCache.put(hashedCursor, continuation);
                    dqr.setCursor(hashedCursor);
                }
                docs = docPage.getContent();
            } else {
                docs = record.findIdsByMetadata_kindAndMetadata_status(kind, status);
            }
            docs.forEach(d -> ids.add(d.getId()));
            dqr.setResults(ids);
            this.logger.info(String.format("Sending ids: %s", ids));
        } catch (CosmosException e) {
            if (e.getStatusCode() == HttpStatus.SC_BAD_REQUEST && e.getMessage().contains("INVALID JSON in continuation token"))
                throw this.getInvalidCursorException();
            else
                throw e;
        } catch (Exception e) {
            throw e;
        }

        return dqr;
    }


    private AppException getInvalidCursorException() {
        return new AppException(HttpStatus.SC_BAD_REQUEST, "Cursor invalid",
                "The requested cursor does not exist or is invalid");
    }}
