package org.opengroup.osdu.storage.api;

import org.opengroup.osdu.core.common.model.storage.StorageRole;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class HealthCheckApi {

    @GetMapping("/health")
    @PreAuthorize("@authorizationFilter.hasRole('" + StorageRole.VIEWER + "', '" + StorageRole.CREATOR + "', '" + StorageRole.ADMIN + "')")
    public String healthMessage() {
        return "Alive";
    }

}
