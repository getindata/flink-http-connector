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

import java.util.BitSet;

/**
 * Low level parser for header field elements. The parsing routines of this class are designed to
 * produce near zero intermediate garbage and make no intermediate copies of input data.
 *
 * <p>This class is immutable and thread safe.
 */
class TokenParser {

    /** US-ASCII CR, carriage return (13). */
    static final char CR = '\r';

    /** US-ASCII LF, line feed (10.). */
    static final char LF = '\n';

    /** US-ASCII SP, space (32). */
    static final char SP = ' ';

    /** US-ASCII HT, horizontal-tab (9). */
    static final char HT = '\t';

    static final TokenParser INSTANCE = new TokenParser();

    static boolean isWhitespace(final char ch) {
        return ch == SP || ch == HT || ch == CR || ch == LF;
    }

    /**
     * Extracts from the sequence of chars a token terminated with any of the given delimiters
     * discarding semantically insignificant whitespace characters.
     *
     * @param buf buffer with the sequence of chars to be parsed
     * @param cursor defines the bounds and current position of the buffer
     * @param delimiters set of delimiting characters. Can be {@code null} if the token is not
     *     delimited by any character.
     */
    String parseToken(
            final CharArrayBuffer buf, final ParserCursor cursor, final BitSet delimiters) {

        final StringBuilder dst = new StringBuilder();
        boolean whitespace = false;
        while (!cursor.atEnd()) {
            final char current = buf.charAt(cursor.getPos());
            if (delimiters != null && delimiters.get(current)) {
                break;
            } else if (isWhitespace(current)) {
                skipWhiteSpace(buf, cursor);
                whitespace = true;
            } else {
                if (whitespace && dst.length() > 0) {
                    dst.append(' ');
                }
                copyContent(buf, cursor, delimiters, dst);
                whitespace = false;
            }
        }
        return dst.toString();
    }

    /**
     * Skips semantically insignificant whitespace characters and moves the cursor to the closest
     * non-whitespace character.
     *
     * @param buf buffer with the sequence of chars to be parsed
     * @param cursor defines the bounds and current position of the buffer
     */
    void skipWhiteSpace(final CharArrayBuffer buf, final ParserCursor cursor) {
        int pos = cursor.getPos();
        final int indexFrom = cursor.getPos();
        final int indexTo = cursor.getUpperBound();
        for (int i = indexFrom; i < indexTo; i++) {
            final char current = buf.charAt(i);
            if (!isWhitespace(current)) {
                break;
            }
            pos++;
        }
        cursor.updatePos(pos);
    }

    /**
     * Transfers content into the destination buffer until a whitespace character or any of the
     * given delimiters is encountered.
     *
     * @param buf buffer with the sequence of chars to be parsed
     * @param cursor defines the bounds and current position of the buffer
     * @param delimiters set of delimiting characters. Can be {@code null} if the value is delimited
     *     by a whitespace only.
     * @param dst destination buffer
     */
    void copyContent(
            final CharArrayBuffer buf,
            final ParserCursor cursor,
            final BitSet delimiters,
            final StringBuilder dst) {
        int pos = cursor.getPos();
        final int indexFrom = cursor.getPos();
        final int indexTo = cursor.getUpperBound();
        for (int i = indexFrom; i < indexTo; i++) {
            final char current = buf.charAt(i);
            if ((delimiters != null && delimiters.get(current)) || isWhitespace(current)) {
                break;
            }
            pos++;
            dst.append(current);
        }
        cursor.updatePos(pos);
    }
}
