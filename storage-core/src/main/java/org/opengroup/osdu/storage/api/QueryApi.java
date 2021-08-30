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

import org.opengroup.osdu.core.common.model.http.AppException;
import org.opengroup.osdu.core.common.model.storage.*;
import org.opengroup.osdu.core.common.model.storage.validation.ValidKind;
import org.opengroup.osdu.storage.di.SchemaEndpointsConfig;
import org.opengroup.osdu.storage.service.BatchService;
import org.opengroup.osdu.storage.util.EncodeDecode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.annotation.RequestScope;

import javax.validation.Valid;

@RestController
@RequestMapping("query")
@RequestScope
@Validated
public class QueryApi {

	@Autowired
	private BatchService batchService;

	@Autowired
	private EncodeDecode encodeDecode;

	@Autowired
	private SchemaEndpointsConfig schemaEndpointsConfig;

	@PostMapping(value = "/records", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	@PreAuthorize("@authorizationFilter.hasRole('" + StorageRole.VIEWER + "', '" + StorageRole.CREATOR + "', '" + StorageRole.ADMIN + "')")
	public ResponseEntity<MultiRecordInfo> getRecords(@Valid @RequestBody MultiRecordIds ids) {
		return new ResponseEntity<MultiRecordInfo>(this.batchService.getMultipleRecords(ids), HttpStatus.OK);
	}

	/**
	 * New fetch records Api, allows maximum 20 records per request and customized header to do conversion.
	 * @param ids id of records to be fetched
	 * @return valid records
	 */
	@PostMapping(value = "/records:batch", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
	@PreAuthorize("@authorizationFilter.hasRole('" + StorageRole.VIEWER + "', '" + StorageRole.CREATOR + "', '" + StorageRole.ADMIN + "')")
	public ResponseEntity<MultiRecordResponse> fetchRecords(@Valid @RequestBody MultiRecordRequest ids) {
		return new ResponseEntity<MultiRecordResponse>(this.batchService.fetchMultipleRecords(ids), HttpStatus.OK);
	}

	// This endpoint is deprecated as of M6, replaced by schema service. In M7 this endpoint will be deleted
	@Deprecated
	@GetMapping(value = "/kinds", produces = MediaType.APPLICATION_JSON_VALUE)
	@PreAuthorize("@authorizationFilter.hasRole('" + StorageRole.CREATOR + "', '" + StorageRole.ADMIN + "')")
	public ResponseEntity<DatastoreQueryResult> getKinds(@RequestParam(required = false) String cursor,
			@RequestParam(required = false) Integer limit) {
        if (!this.schemaEndpointsConfig.isDisabled()) {
			DatastoreQueryResult result = this.batchService.getAllKinds(encodeDecode.deserializeCursor(cursor), limit);
            result.setCursor(encodeDecode.serializeCursor(result.getCursor()));
            return new ResponseEntity<DatastoreQueryResult>(result, HttpStatus.OK);
        } else {
			throw new AppException(org.apache.http.HttpStatus.SC_NOT_FOUND,"This API has been deprecated","Unable to perform action");
        }
	}

	@GetMapping(value = "/records", produces = MediaType.APPLICATION_JSON_VALUE)
	@PreAuthorize("@authorizationFilter.hasRole('" + StorageRole.ADMIN + "')")
	public ResponseEntity<DatastoreQueryResult> getAllRecords(
			@RequestParam(required = false) String cursor,
			@RequestParam(required = false) Integer limit,
			@RequestParam @ValidKind String kind) {
		DatastoreQueryResult result = this.batchService.getAllRecords(encodeDecode.deserializeCursor(cursor), kind, limit);
		result.setCursor(encodeDecode.serializeCursor(result.getCursor()));
		return new ResponseEntity<DatastoreQueryResult>(result, HttpStatus.OK);
	}
}