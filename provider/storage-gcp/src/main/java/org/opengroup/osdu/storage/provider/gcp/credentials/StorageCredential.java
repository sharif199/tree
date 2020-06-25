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

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.Date;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.http.HttpStatus;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.common.base.Strings;
import com.google.api.services.iam.v1.Iam;
import com.google.api.services.iam.v1.Iam.Projects.ServiceAccounts.SignJwt;
import com.google.api.services.iam.v1.model.SignJwtRequest;
import com.google.api.services.storage.StorageScopes;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.net.HttpHeaders;
import com.google.gson.JsonObject;
import org.opengroup.osdu.core.common.http.HttpClient;
import org.opengroup.osdu.core.common.http.HttpRequest;
import org.opengroup.osdu.core.common.http.HttpResponse;
import org.opengroup.osdu.core.common.model.http.AppException;
import com.google.auth.http.HttpCredentialsAdapter;

public class StorageCredential extends GoogleCredentials {
	private static final long serialVersionUID = -8461791038757192780L;
	private static final String JWT_AUDIENCE = "https://www.googleapis.com/oauth2/v4/token";
	private static final String SERVICE_ACCOUNT_NAME_FORMAT = "projects/-/serviceAccounts/%s";
	private static final JsonFactory JSON_FACTORY = new JacksonFactory();
	private transient Iam iam;

	private String email;
	private String serviceAccount;
	private final transient HttpClient httpClient;

	public StorageCredential(String email, String serviceAccount) {
		this(email, serviceAccount, new HttpClient());
	}

	protected StorageCredential(String email, String serviceAccount, HttpClient httpClient) {
		this.email = email;
		this.serviceAccount = serviceAccount;
		this.httpClient = httpClient;
	}

	@Override
	public AccessToken refreshAccessToken() {

		String signedJwt = this.signJwt();

		return this.exchangeForAccessToken(signedJwt);
	}

	private String signJwt() {
		boolean isServiceAccount = StringUtils.endsWithIgnoreCase(this.email, "gserviceaccount.com");
		String issuer = isServiceAccount ? this.email : this.serviceAccount;
		String subject = isServiceAccount
				? ""
				: this.email;
		String signingServiceAccountEmail = isServiceAccount
				? this.email
				: this.serviceAccount;

		try {
			SignJwtRequest signJwtRequest = new SignJwtRequest();
			signJwtRequest.setPayload(this.getPayload(issuer, subject));

			SignJwt signJwt = this.getIam().projects().serviceAccounts()
					.signJwt(String.format(SERVICE_ACCOUNT_NAME_FORMAT, signingServiceAccountEmail), signJwtRequest);

			return signJwt.execute().getSignedJwt();

		} catch (Exception e) {
			throw new AppException(HttpStatus.SC_INTERNAL_SERVER_ERROR, "Internal server error",
					"An error occurred when accessing third-party APIs", e);
		}
	}

	private AccessToken exchangeForAccessToken(String signedJwt) {
		HttpRequest request = HttpRequest.post().url(JWT_AUDIENCE)
				.headers(Collections.singletonMap(HttpHeaders.CONTENT_TYPE, "application/x-www-form-urlencoded"))
				.body(String.format("%s=%s&%s=%s", "grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer",
						"assertion", signedJwt))
				.build();
		HttpResponse response = this.httpClient.send(request);
		JsonObject jsonResult = response.getAsJsonObject();

		if (!response.isSuccessCode() || !jsonResult.has("access_token")) {
			throw new AppException(HttpStatus.SC_INTERNAL_SERVER_ERROR, "Internal server error",
					"An error occurred when accessing third-party APIs");
		}

		return new AccessToken(jsonResult.get("access_token").getAsString(),
				DateUtils.addSeconds(new Date(), jsonResult.get("expires_in").getAsInt()));
	}

	private String getPayload(String issuer, String subject) {
		JsonObject payload = new JsonObject();

		if (!Strings.isNullOrEmpty(subject)) {
			payload.addProperty("sub", subject);
		}

		payload.addProperty("iss", issuer);
		payload.addProperty("scope", StorageScopes.DEVSTORAGE_FULL_CONTROL);
		payload.addProperty("aud", JWT_AUDIENCE);
		payload.addProperty("iat", System.currentTimeMillis() / 1000);

		return payload.toString();
	}

	protected void setIam(Iam iam) {
		this.iam = iam;
	}

	private Iam getIam() throws GeneralSecurityException, IOException {
		if (this.iam == null) {
			Iam.Builder builder = new Iam.Builder(GoogleNetHttpTransport.newTrustedTransport(), JSON_FACTORY,
					new HttpCredentialsAdapter(GoogleCredentials.getApplicationDefault()))
					.setApplicationName("DPS Storage Service");

			this.iam = builder.build();
		}
		return this.iam;
	}
}