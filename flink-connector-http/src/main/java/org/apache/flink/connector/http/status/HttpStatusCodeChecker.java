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

/**
 * Base interface for all classes that would validate HTTP status code whether it is an error or
 * not.
 */
public interface HttpStatusCodeChecker {

    /**
     * Validates http status code wheter it is considered as error code. The logic for what status
     * codes are considered as "errors" depends on the concreted implementation
     *
     * @param statusCode http status code to assess.
     * @return true if statusCode is considered as Error and false if not.
     */
    boolean isErrorCode(int statusCode);
}
