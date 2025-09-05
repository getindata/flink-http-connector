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

package org.apache.flink.connector.http.utils;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import java.lang.Thread.UncaughtExceptionHandler;

import static org.apache.flink.connector.http.utils.ExceptionUtils.stringifyException;

/** thread utils . */
@UtilityClass
@Slf4j
@NoArgsConstructor(access = AccessLevel.NONE)
public final class ThreadUtils {

    public static final UncaughtExceptionHandler LOGGING_EXCEPTION_HANDLER =
            (t, e) -> log.warn("Thread:" + t + " exited with Exception:" + stringifyException(e));
}
