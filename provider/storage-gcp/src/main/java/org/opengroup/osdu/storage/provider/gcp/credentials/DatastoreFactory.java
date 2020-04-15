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

package org.opengroup.osdu.storage.provider.gcp.credentials;

import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import org.opengroup.osdu.core.common.cache.ICache;
import org.opengroup.osdu.core.common.util.Crc32c;
import org.opengroup.osdu.core.common.model.tenant.TenantInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.threeten.bp.Duration;

@Component
public class DatastoreFactory implements IDatastoreFactory {

	private static final RetrySettings RETRY_SETTINGS = RetrySettings.newBuilder()
			.setMaxAttempts(6)
			.setInitialRetryDelay(Duration.ofSeconds(1))
			.setMaxRetryDelay(Duration.ofSeconds(10))
			.setRetryDelayMultiplier(2.0)
			.setTotalTimeout(Duration.ofSeconds(50))
			.setInitialRpcTimeout(Duration.ofSeconds(50))
			.setRpcTimeoutMultiplier(1.1)
			.setMaxRpcTimeout(Duration.ofSeconds(50))
			.build();

	@Autowired
	private TenantInfo tenant;

	@Autowired
	private ICache<String, DatastoreCredential> credentialsCache;

	@Override
	public Datastore getDatastore() {

		String cacheKey = this.getCacheKey();

		DatastoreCredential credential = this.credentialsCache.get(cacheKey);

		if (credential == null) {
			credential = new DatastoreCredential(this.tenant);
			this.credentialsCache.put(cacheKey, credential);
		}

		return DatastoreOptions.newBuilder()
				.setRetrySettings(RETRY_SETTINGS)
				.setCredentials(credential)
				.setProjectId(this.tenant.getProjectId())
				.build()
				.getService();
	}

	private String getCacheKey() {
		return Crc32c.hashToBase64EncodedString(String.format("datastoreCredential:%s", this.tenant.getName()));
	}
}