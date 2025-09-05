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

package org.apache.flink.connector.http.sink;

import org.apache.flink.connector.http.HttpSink;

import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import java.io.Serializable;

/**
 * Represents a single {@link HttpSink} request. Contains the HTTP method name, Content-Type header
 * value, and byte representation of the body of the request.
 */
@RequiredArgsConstructor
@EqualsAndHashCode
@ToString
public final class HttpSinkRequestEntry implements Serializable {

    /** HTTP method name to use when sending the request. */
    @NonNull public final String method;

    /** Body of the request, encoded as byte array. */
    public final byte[] element;

    /** @return the size of the {@link HttpSinkRequestEntry#element} */
    public long getSizeInBytes() {
        return element.length;
    }
}
