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

package org.opengroup.osdu.storage.api;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import org.opengroup.osdu.core.common.model.http.AppException;
import org.opengroup.osdu.core.common.model.storage.validation.ValidKind;
import org.opengroup.osdu.storage.di.SchemaEndpointsConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Component;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import org.opengroup.osdu.core.common.model.storage.Schema;
import org.opengroup.osdu.core.common.model.storage.StorageRole;
import org.opengroup.osdu.storage.service.SchemaService;
import org.springframework.web.context.annotation.RequestScope;

@RestController
@RequestMapping("schemas")
@RequestScope
@Validated
public class SchemaApi {
	private SchemaService schemaService;

	@Autowired
	private SchemaEndpointsConfig schemaEndpointsConfig;

	// @InjectMocks and @Autowired together only work on setter DI
	@Autowired
	public void setSchemaService(SchemaService schemaService)
	{
		this.schemaService = schemaService;
	}

	// This endpoint is deprecated as of M6, replaced by schema service.  In M7 this endpoint will be deleted
	@Deprecated
	@PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	@PreAuthorize("@authorizationFilter.hasRole('" + StorageRole.CREATOR + "', '" + StorageRole.ADMIN + "')")
	public ResponseEntity<Void> createSchema(@Valid @NotNull @RequestBody Schema schema)
	{
		if (!this.schemaEndpointsConfig.isDisabled()) {
			this.schemaService.createSchema(schema);
			return new ResponseEntity<Void>(HttpStatus.CREATED);
		} else {
			throw new AppException(org.apache.http.HttpStatus.SC_NOT_FOUND,"This API has been deprecated","Unable to perform action");
		}

	}

	// This endpoint is deprecated as of M6, replaced by schema service.  In M7 this endpoint will be deleted
	@Deprecated
	@GetMapping(value = "/{kind}", produces = MediaType.APPLICATION_JSON_VALUE)
	@PreAuthorize("@authorizationFilter.hasRole('" + StorageRole.VIEWER + "', '" + StorageRole.CREATOR + "', '" + StorageRole.ADMIN + "')")
	public ResponseEntity<Schema> getSchema(@PathVariable("kind") @ValidKind String kind) {
		if (!this.schemaEndpointsConfig.isDisabled()) {
			return new ResponseEntity<Schema>(this.schemaService.getSchema(kind), HttpStatus.OK);
		} else {
			throw new AppException(org.apache.http.HttpStatus.SC_NOT_FOUND,"This API has been deprecated","Unable to perform action");
		}
	}

	// This endpoint is deprecated as of M6. In M7 this endpoint will be deleted
	@Deprecated
	@DeleteMapping(value = "/{kind}", produces = MediaType.APPLICATION_JSON_VALUE)
	@PreAuthorize("@authorizationFilter.hasRole('" + StorageRole.ADMIN + "')")
	public ResponseEntity<Void> deleteSchema(@PathVariable("kind") @ValidKind String kind) {
		if (!this.schemaEndpointsConfig.isDisabled()) {
			this.schemaService.deleteSchema(kind);
			return new ResponseEntity<Void>(HttpStatus.NO_CONTENT);
		} else {
			throw new AppException(org.apache.http.HttpStatus.SC_NOT_FOUND,"This API has been deprecated","Unable to perform action");
		}
	}
}