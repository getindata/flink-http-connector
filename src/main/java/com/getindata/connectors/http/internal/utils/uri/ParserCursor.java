/*
 * ====================================================================
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * ====================================================================
 *
 * This software consists of voluntary contributions made by many
 * individuals on behalf of the Apache Software Foundation.  For more
 * information on the Apache Software Foundation, please see
 * <http://www.apache.org/>.
 *
 * ============================= NOTE =================================
 * This code has been copied from
 * https://github.com/apache/httpcomponents-client/tree/rel/v4.5.13
 * and it was changed to use in this project.
 * ====================================================================
 */

package com.getindata.connectors.http.internal.utils.uri;

import lombok.Getter;

/**
 * This class represents a context of a parsing operation:
 * <ul>
 *  <li>the current position the parsing operation is expected to start at</li>
 *  <li>the bounds limiting the scope of the parsing operation</li>
 * </ul>
 */
@Getter
class ParserCursor {

    private final int lowerBound;

    private final int upperBound;

    private int pos;

    ParserCursor(final int lowerBound, final int upperBound) {
        super();
        if (lowerBound < 0) {
            throw new IndexOutOfBoundsException("Lower bound cannot be negative");
        }
        if (lowerBound > upperBound) {
            throw new IndexOutOfBoundsException("Lower bound cannot be greater then upper bound");
        }
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.pos = lowerBound;
    }

    void updatePos(final int pos) {
        if (pos < this.lowerBound) {
            throw new IndexOutOfBoundsException(
                "pos: " + pos + " < lowerBound: " + this.lowerBound);
        }
        if (pos > this.upperBound) {
            throw new IndexOutOfBoundsException(
                "pos: " + pos + " > upperBound: " + this.upperBound);
        }
        this.pos = pos;
    }

    boolean atEnd() {
        return this.pos >= this.upperBound;
    }

    @Override
    public String toString() {
        return "["
            + this.lowerBound
            + '>'
            + this.pos
            + '>'
            + this.upperBound
            + ']';
    }
}
