/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.http.status;

import org.apache.flink.util.ConfigurationException;

import lombok.Getter;
import lombok.NonNull;

import java.net.http.HttpResponse;
import java.util.HashSet;
import java.util.Set;

/** Http response checker. */
@Getter
public class HttpResponseChecker {

    private final Set<Integer> successCodes;
    private final Set<Integer> temporalErrorCodes;

    HttpResponseChecker(@NonNull String successCodeExpr, @NonNull String temporalErrorCodeExpr)
            throws ConfigurationException {
        this(HttpCodesParser.parse(successCodeExpr), HttpCodesParser.parse(temporalErrorCodeExpr));
    }

    public HttpResponseChecker(
            @NonNull Set<Integer> successCodes, @NonNull Set<Integer> temporalErrorCodes)
            throws ConfigurationException {
        this.successCodes = successCodes;
        this.temporalErrorCodes = temporalErrorCodes;
        validate();
    }

    public boolean isSuccessful(HttpResponse<?> response) {
        return isSuccessful(response.statusCode());
    }

    public boolean isSuccessful(int httpStatusCode) {
        return successCodes.contains(httpStatusCode);
    }

    public boolean isTemporalError(HttpResponse<?> response) {
        return isTemporalError(response.statusCode());
    }

    public boolean isTemporalError(int httpStatusCode) {
        return temporalErrorCodes.contains(httpStatusCode);
    }

    private void validate() throws ConfigurationException {
        if (successCodes.isEmpty()) {
            throw new ConfigurationException("Success code list can not be empty");
        }
        var intersection = new HashSet<>(successCodes);
        intersection.retainAll(temporalErrorCodes);
        if (!intersection.isEmpty()) {
            throw new ConfigurationException(
                    "Http codes "
                            + intersection
                            + " can not be used as both success and retry codes");
        }
    }
}
