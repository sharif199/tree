/* Licensed Materials - Property of IBM              */
/* (c) Copyright IBM Corp. 2020. All Rights Reserved.*/

package org.opengroup.osdu.storage.provider.ibm.util;

import org.apache.http.HttpStatus;
import org.opengroup.osdu.core.common.model.http.AppException;
import org.opengroup.osdu.core.common.util.IServiceAccountJwtClient;
import org.opengroup.osdu.core.ibm.multitenancy.PartitonServiceTokenService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.context.annotation.RequestScope;

@Component
@RequestScope
public class ServiceAccountJwtClientImpl implements IServiceAccountJwtClient {

	@Autowired
	PartitonServiceTokenService tokenService;
	
	@Override
	public String getIdToken(String tenantName) {
		try {
			return tokenService.getIdToken();
		} catch (Exception e) {
			throw new AppException(HttpStatus.SC_INTERNAL_SERVER_ERROR, "Persistence error", "Error generating token",e);
		}
	}
}
