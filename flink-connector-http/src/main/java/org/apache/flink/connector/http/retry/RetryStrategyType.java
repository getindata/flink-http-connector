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

package org.apache.flink.connector.http.retry;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/** Retry strategy type enum. */
@Getter
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public enum RetryStrategyType {
    FIXED_DELAY("fixed-delay"),
    EXPONENTIAL_DELAY("exponential-delay"),
    ;

    private final String code;

    public static RetryStrategyType fromCode(String code) {
        if (code == null) {
            throw new NullPointerException("Code is null");
        }
        for (var strategy : RetryStrategyType.values()) {
            if (strategy.getCode().equalsIgnoreCase(code)) {
                return strategy;
            }
        }
        throw new IllegalArgumentException("No enum constant for " + code);
    }
}
