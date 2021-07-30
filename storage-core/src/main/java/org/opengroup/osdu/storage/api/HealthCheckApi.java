package org.opengroup.osdu.storage.api;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class HealthCheckApi {

    @GetMapping("/health")
    @PreAuthorize("@authorizationFilter.hasRole('" + StorageRole.ADMIN + "')")
    public String healthMessage() {
        return "Alive";
    }

}
