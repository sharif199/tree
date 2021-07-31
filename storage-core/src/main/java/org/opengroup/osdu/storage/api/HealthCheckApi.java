package org.opengroup.osdu.storage.api;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.security.access.prepost.PreAuthorize;
import org.opengroup.osdu.core.common.model.storage.StorageRole;

@RestController
public class HealthCheckApi {

    @GetMapping("/health")
    @PreAuthorize("@authorizationFilter.hasRole('" + StorageRole.ADMIN + "')")
    public String healthMessage() {
        return "Alive";
    }

    @GetMapping("/healthh")
    @PreAuthorize("@authorizationFilter.hasRole('" + StorageRole.ADMIN + "')")
    public String healthMessage2() {
        return "Alive";
    }
}
