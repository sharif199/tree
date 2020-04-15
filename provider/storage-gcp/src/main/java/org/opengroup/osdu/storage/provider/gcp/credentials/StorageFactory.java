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
import com.google.cloud.TransportOptions;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.opengroup.osdu.core.common.cache.ICache;
import org.opengroup.osdu.core.common.util.Crc32c;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.threeten.bp.Duration;

@Component
public class StorageFactory implements IStorageFactory {

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

	private static final TransportOptions TRANSPORT_OPTIONS = HttpTransportOptions.newBuilder()
			.setReadTimeout(40 * 1000)
			.setConnectTimeout(10 * 1000)
			.build();

	@Autowired
	private ICache<String, StorageCredential> credentialsCache;

	@Override
	public Storage getStorage(String userId, String serviceAccount, String projectId, String tenantName) {
		String cacheKey = this.getCredentialsCacheKey(userId, tenantName);

		StorageCredential credential = this.credentialsCache.get(cacheKey);

		if (credential == null) {
			credential = new StorageCredential(userId, serviceAccount);
			this.credentialsCache.put(cacheKey, credential);
		}

		return StorageOptions.newBuilder()
				.setCredentials(credential)
				.setProjectId(projectId)
				.setRetrySettings(RETRY_SETTINGS)
				.setTransportOptions(TRANSPORT_OPTIONS)
				.build()
				.getService();
	}

	private String getCredentialsCacheKey(String email, String tenantName) {
		return Crc32c.hashToBase64EncodedString(
				String.format("storageCredential:%s:%s", tenantName, email));
	}
}