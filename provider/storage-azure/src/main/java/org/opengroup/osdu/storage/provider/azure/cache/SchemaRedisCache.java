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

package org.opengroup.osdu.storage.provider.azure.cache;

import org.opengroup.osdu.core.common.cache.RedisCache;
import org.opengroup.osdu.core.common.logging.JaxRsDpsLog;
import org.opengroup.osdu.core.common.model.entitlements.Groups;
import org.opengroup.osdu.core.common.model.storage.Schema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import javax.inject.Named;

@Component
@ConditionalOnProperty(value = "runtime.env.local", havingValue = "false", matchIfMissing = true)
public class SchemaRedisCache extends RedisCache<String, Schema> {

    private JaxRsDpsLog logger;

    public SchemaRedisCache(
            final @Named("REDIS_HOST") String host,
            final @Named("REDIS_PORT") int port,
            final @Named("REDIS_PASSWORD") String password,
            final @Named("SCHEMA_REDIS_TTL") int timeout,
            @Value("${redis.database}") final int database,
            final @Autowired JaxRsDpsLog logger)
    {
        super(host, port, password, timeout, database, String.class, Schema.class);
        this.logger = logger;
    }

    @Override
    public Schema get(String key) {
        try {
            return super.get(key);
        } catch (Exception ex) {
            this.logger.error(String.format("Error getting key %s from redis: %s",
                    key, ex));
            return null;
        }
    }
}