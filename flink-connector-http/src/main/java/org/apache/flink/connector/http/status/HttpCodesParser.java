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

import lombok.experimental.UtilityClass;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.lang.String.format;

/** Utility class for parsing http codes. */
@UtilityClass
public class HttpCodesParser {

    private static final Pattern CODE_GROUP_EXPRESSION = Pattern.compile("[1-5][xX]{2}");
    private static final String DELIMITER = Pattern.quote(",");
    private static final int HTTP_CODE_MIN = 100;
    private static final int HTTP_CODE_MAX = 599;

    public Set<Integer> parse(String codesExpression) throws ConfigurationException {
        var includelist = new HashSet<Integer>();
        var excludelist = new HashSet<Integer>();
        for (var rawCode : codesExpression.split(DELIMITER)) {
            var code = rawCode.trim();
            if (code.isEmpty()) {
                continue;
            }
            if (code.startsWith("!")) {
                try {
                    excludelist.add(parseHttpCode(code.substring(1)));
                    continue;
                } catch (NumberFormatException e) {
                    throw new ConfigurationException("Can not parse code " + code);
                }
            }
            try {
                includelist.add(parseHttpCode(code));
            } catch (NumberFormatException e) {
                if (CODE_GROUP_EXPRESSION.matcher(code).matches()) {
                    var firstGroupCode = Integer.parseInt(code.substring(0, 1)) * 100;
                    var groupCodes =
                            IntStream.range(firstGroupCode, firstGroupCode + 100)
                                    .boxed()
                                    .collect(Collectors.toList());
                    includelist.addAll(groupCodes);
                } else {
                    throw new ConfigurationException("Can not parse code " + code);
                }
            }
        }

        includelist.removeAll(excludelist);
        return Collections.unmodifiableSet(includelist);
    }

    private Integer parseHttpCode(String str) throws ConfigurationException {
        var parsed = Integer.parseInt(str);
        if (parsed < HTTP_CODE_MIN || parsed > HTTP_CODE_MAX) {
            throw new ConfigurationException(format("Http code out of the range [%s]", parsed));
        }
        return parsed;
    }
}
