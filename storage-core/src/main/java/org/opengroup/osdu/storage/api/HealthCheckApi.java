package org.opengroup.osdu.storage.api;

import org.opengroup.osdu.core.common.http.HttpResponse;
import org.opengroup.osdu.core.common.http.IHttpClient;
import org.opengroup.osdu.core.common.model.storage.StorageRole;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.opengroup.osdu.core.common.http.HttpRequest;

import java.util.Random;

@RestController
public class HealthCheckApi {

    @Autowired
    IHttpClient httpClient;


    @GetMapping("/health")
    @PreAuthorize("@authorizationFilter.hasRole('" + StorageRole.ADMIN + "')")
    public String healthMessage() {
        HttpRequest httpRequest = HttpRequest.get().url("http://legal/api/legal/v1/_ah/liveness_check").build();
        return httpClient.send(httpRequest).getBody();
    }

    @GetMapping("/healthh")
    @PreAuthorize("@authorizationFilter.hasRole('" + StorageRole.ADMIN + "')")
    public String healthMessagee() {
        return "Alive";
    }
}
