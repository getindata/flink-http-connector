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

/*
 * ============================= NOTE =================================
 * This code has been copied from
 * https://github.com/apache/httpcomponents-client/tree/rel/v4.5.13
 */

package org.apache.flink.connector.http.utils.uri;

import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.nio.CharBuffer;

/** A resizable char array. */
final class CharArrayBuffer implements CharSequence, Serializable {

    private static final long serialVersionUID = -6208952725094867135L;

    private char[] buffer;

    private int len;

    /**
     * Creates an instance of {@link CharArrayBuffer} with the given initial capacity.
     *
     * @param capacity the capacity
     */
    CharArrayBuffer(final int capacity) {
        super();
        Preconditions.checkArgument(capacity > 0, "Buffer capacity must be bigger than 0.");
        this.buffer = new char[capacity];
    }

    /**
     * Appends chars of the given string to this buffer. The capacity of the buffer is increased, if
     * necessary, to accommodate all chars.
     *
     * @param str the string.
     */
    void append(final String str) {
        final String s = str != null ? str : "null";
        final int strLen = s.length();
        final int newLen = this.len + strLen;
        if (newLen > this.buffer.length) {
            expand(newLen);
        }
        s.getChars(0, strLen, this.buffer, this.len);
        this.len = newLen;
    }

    /**
     * Returns the {@code char} value in this buffer at the specified index. The index argument must
     * be greater than or equal to {@code 0}, and less than the length of this buffer.
     *
     * @param i the index of the desired char value.
     * @return the char value at the specified index.
     * @throws IndexOutOfBoundsException if {@code index} is negative or greater than or equal to
     *     {@link #length()}.
     */
    @Override
    public char charAt(final int i) {
        return this.buffer[i];
    }

    /**
     * Returns the length of the buffer (char count).
     *
     * @return the length of the buffer
     */
    @Override
    public int length() {
        return this.len;
    }

    @Override
    public CharSequence subSequence(final int beginIndex, final int endIndex) {
        if (beginIndex < 0) {
            throw new IndexOutOfBoundsException("Negative beginIndex: " + beginIndex);
        }
        if (endIndex > this.len) {
            throw new IndexOutOfBoundsException("endIndex: " + endIndex + " > length: " + this.len);
        }
        if (beginIndex > endIndex) {
            throw new IndexOutOfBoundsException(
                    "beginIndex: " + beginIndex + " > endIndex: " + endIndex);
        }
        return CharBuffer.wrap(this.buffer, beginIndex, endIndex);
    }

    private void expand(final int newLen) {
        final char[] newBuffer = new char[Math.max(this.buffer.length << 1, newLen)];
        System.arraycopy(this.buffer, 0, newBuffer, 0, this.len);
        this.buffer = newBuffer;
    }

    @Override
    public String toString() {
        return new String(this.buffer, 0, this.len);
    }
}
