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

package org.apache.flink.connector.http.preprocessor;

import java.io.Serializable;

/**
 * Processor interface which modifies header value based on implemented logic. An example would be
 * calculation of Value of Authorization header.
 */
public interface HeaderValuePreprocessor extends Serializable {

    /**
     * Modifies header rawValue according to the implemented logic.
     *
     * @param rawValue header original value to modify
     * @return modified header value.
     */
    String preprocessHeaderValue(String rawValue);
}
