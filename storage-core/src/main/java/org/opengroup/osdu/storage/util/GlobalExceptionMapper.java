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

package org.opengroup.osdu.storage.util;

import static org.apache.http.HttpStatus.SC_MULTI_STATUS;

import javax.validation.ValidationException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import javassist.NotFoundException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.opengroup.osdu.core.common.logging.JaxRsDpsLog;
import org.opengroup.osdu.storage.exception.DeleteRecordsException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.lang.NonNull;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.WebRequest;
import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;
import org.opengroup.osdu.core.common.model.http.AppException;

import java.io.IOException;

@Order(Ordered.HIGHEST_PRECEDENCE)
@ControllerAdvice
public class GlobalExceptionMapper extends ResponseEntityExceptionHandler {

    @Autowired
    private JaxRsDpsLog logger;

    @ExceptionHandler(AppException.class)
    protected ResponseEntity<Object> handleAppException(AppException e) {
        return this.getErrorResponse(e);
    }

    @ExceptionHandler(ValidationException.class)
    protected ResponseEntity<Object> handleValidationException(ValidationException e) {
        return this.getErrorResponse(
                new AppException(HttpStatus.BAD_REQUEST.value(), "Validation error.", e.getMessage(), e));
    }

    @ExceptionHandler(NotFoundException.class)
    protected ResponseEntity<Object> handleNotFoundException(NotFoundException e) {
        return this.getErrorResponse(
                new AppException(HttpStatus.NOT_FOUND.value(), "Resource not found.", e.getMessage(), e));
    }

    @ExceptionHandler(UnrecognizedPropertyException.class)
    protected ResponseEntity<Object> handleUnrecognizedPropertyException(UnrecognizedPropertyException e) {
        return this.getErrorResponse(
                new AppException(HttpStatus.BAD_REQUEST.value(), "Unrecognized property.", e.getMessage(), e));
    }

    @ExceptionHandler(JsonProcessingException.class)
    protected ResponseEntity<Object> handleJsonProcessingException(JsonProcessingException e) {
        return this.getErrorResponse(
                new AppException(HttpStatus.BAD_REQUEST.value(), "Failed to process JSON.", e.getMessage(), e));
    }

    @ExceptionHandler(AccessDeniedException.class)
    protected ResponseEntity<Object> handleAccessDeniedException(AccessDeniedException e) {
        return this.getErrorResponse(
                new AppException(HttpStatus.FORBIDDEN.value(), "Access denied", e.getMessage(), e));
    }

    @ExceptionHandler(IOException.class)
    public ResponseEntity<Object> handleIOException(IOException e) {
        if (StringUtils.containsIgnoreCase(ExceptionUtils.getRootCauseMessage(e), "Broken pipe")) {
            this.logger.warning("Client closed the connection while request still being processed");
            return null;
        } else {
            return this.getErrorResponse(
                    new AppException(HttpStatus.SERVICE_UNAVAILABLE.value(), "Unknown error", e.getMessage(), e));
        }
    }

    @ExceptionHandler(DeleteRecordsException.class)
    protected ResponseEntity<Object> handleDeleteRecordsException(DeleteRecordsException e) {
        JsonArray responseArray = new JsonArray();

        e.getNotDeletedRecords().stream()
                .map(pair -> {
                    JsonObject jsonObject = new JsonObject();
                    jsonObject.add("notDeletedRecordId", new JsonPrimitive(pair.getKey()));
                    jsonObject.add("message", new JsonPrimitive(pair.getValue()));
                    return jsonObject;
                })
                .forEach(responseArray::add);
        return ResponseEntity.status(SC_MULTI_STATUS).body(responseArray.toString());
    }

    @Override
    @NonNull
    protected ResponseEntity<Object> handleHttpRequestMethodNotSupported(@NonNull HttpRequestMethodNotSupportedException e,
                                                                         @NonNull HttpHeaders headers,
                                                                         @NonNull HttpStatus status,
                                                                         @NonNull WebRequest request) {
        return this.getErrorResponse(
                new AppException(org.apache.http.HttpStatus.SC_METHOD_NOT_ALLOWED, "Method not found.",
                        "Method not found.", e));
    }

    public ResponseEntity<Object> getErrorResponse(AppException e) {

        String exceptionMsg = e.getOriginalException() != null
                ? e.getOriginalException().getMessage()
                : e.getError().getMessage();

        if (e.getError().getCode() > 499) {
            this.logger.error(exceptionMsg, e);
        } else {
            this.logger.warning(exceptionMsg, e);
        }

        return new ResponseEntity<Object>(e.getError(), HttpStatus.resolve(e.getError().getCode()));
    }
}