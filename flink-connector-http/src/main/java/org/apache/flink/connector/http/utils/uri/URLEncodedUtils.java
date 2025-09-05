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

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;

/** A collection of utilities for encoding URLs. */
public class URLEncodedUtils {

    private static final char QP_SEP_A = '&';

    private static final char QP_SEP_S = ';';

    private static final String NAME_VALUE_SEPARATOR = "=";

    private static final char PATH_SEPARATOR = '/';

    private static final BitSet PATH_SEPARATORS = new BitSet(256);
    /**
     * Unreserved characters, i.e. alphanumeric, plus: {@code _ - ! . ~ ' ( ) *}
     *
     * <p>This list is the same as the {@code unreserved} list in <a
     * href="http://www.ietf.org/rfc/rfc2396.txt">RFC 2396</a>
     */
    private static final BitSet UNRESERVED = new BitSet(256);
    /**
     * Punctuation characters: , ; : $ & + =
     *
     * <p>These are the additional characters allowed by userinfo.
     */
    private static final BitSet PUNCT = new BitSet(256);
    /**
     * Characters which are safe to use in userinfo, i.e. {@link #UNRESERVED} plus {@link
     * #PUNCT}uation
     */
    private static final BitSet USERINFO = new BitSet(256);
    /**
     * Characters which are safe to use in a path, i.e. {@link #UNRESERVED} plus {@link
     * #PUNCT}uation plus / @
     */
    private static final BitSet PATHSAFE = new BitSet(256);
    /**
     * Characters which are safe to use in a query or a fragment, i.e. {@link #RESERVED} plus {@link
     * #UNRESERVED}
     */
    private static final BitSet URIC = new BitSet(256);
    /**
     * Reserved characters, i.e. {@code ;/?:@&=+$,[]}
     *
     * <p>This list is the same as the {@code reserved} list in <a
     * href="http://www.ietf.org/rfc/rfc2396.txt">RFC 2396</a> as augmented by <a
     * href="http://www.ietf.org/rfc/rfc2732.txt">RFC 2732</a>
     */
    private static final BitSet RESERVED = new BitSet(256);
    /**
     * Safe characters for x-www-form-urlencoded data, as per java.net.URLEncoder and browser
     * behaviour, i.e. alphanumeric plus {@code "-", "_", ".", "*"}
     */
    private static final BitSet URLENCODER = new BitSet(256);

    private static final BitSet PATH_SPECIAL = new BitSet(256);

    private static final int RADIX = 16;

    static {
        PATH_SEPARATORS.set(PATH_SEPARATOR);
    }

    static {
        // unreserved chars
        // alpha characters
        for (int i = 'a'; i <= 'z'; i++) {
            UNRESERVED.set(i);
        }
        for (int i = 'A'; i <= 'Z'; i++) {
            UNRESERVED.set(i);
        }
        // numeric characters
        for (int i = '0'; i <= '9'; i++) {
            UNRESERVED.set(i);
        }
        UNRESERVED.set('_'); // these are the charactes of the "mark" list
        UNRESERVED.set('-');
        UNRESERVED.set('.');
        UNRESERVED.set('*');
        URLENCODER.or(UNRESERVED); // skip remaining unreserved characters
        UNRESERVED.set('!');
        UNRESERVED.set('~');
        UNRESERVED.set('\'');
        UNRESERVED.set('(');
        UNRESERVED.set(')');
        // punct chars
        PUNCT.set(',');
        PUNCT.set(';');
        PUNCT.set(':');
        PUNCT.set('$');
        PUNCT.set('&');
        PUNCT.set('+');
        PUNCT.set('=');
        // Safe for userinfo
        USERINFO.or(UNRESERVED);
        USERINFO.or(PUNCT);

        // URL path safe
        PATHSAFE.or(UNRESERVED);
        PATHSAFE.set(';'); // param separator
        PATHSAFE.set(':'); // RFC 2396
        PATHSAFE.set('@');
        PATHSAFE.set('&');
        PATHSAFE.set('=');
        PATHSAFE.set('+');
        PATHSAFE.set('$');
        PATHSAFE.set(',');

        PATH_SPECIAL.or(PATHSAFE);
        PATH_SPECIAL.set('/');

        RESERVED.set(';');
        RESERVED.set('/');
        RESERVED.set('?');
        RESERVED.set(':');
        RESERVED.set('@');
        RESERVED.set('&');
        RESERVED.set('=');
        RESERVED.set('+');
        RESERVED.set('$');
        RESERVED.set(',');
        RESERVED.set('['); // added by RFC 2732
        RESERVED.set(']'); // added by RFC 2732

        URIC.or(RESERVED);
        URIC.or(UNRESERVED);
    }

    /**
     * Returns a list of {@link NameValuePair}s URI query parameters. By convention, {@code '&'} and
     * {@code ';'} are accepted as parameter separators.
     *
     * @param queryComponent URI query component.
     * @param charset charset to use when decoding the parameters.
     * @return list of query parameters.
     */
    static List<NameValuePair> parse(final String queryComponent, final Charset charset) {
        if (queryComponent == null) {
            return createEmptyList();
        }
        final CharArrayBuffer buffer = new CharArrayBuffer(queryComponent.length());
        buffer.append(queryComponent);
        return parse(buffer, charset, QP_SEP_A, QP_SEP_S);
    }

    /**
     * Returns a list of {@link NameValuePair}s parameters.
     *
     * @param buf text to parse.
     * @param charset Encoding to use when decoding the parameters.
     * @param separators element separators.
     * @return a list of {@link NameValuePair} as built from the URI's query portion.
     */
    static List<NameValuePair> parse(
            final CharArrayBuffer buf, final Charset charset, final char... separators) {
        Preconditions.checkNotNull(buf, "Char array buffer cannot be null.");
        final TokenParser tokenParser = TokenParser.INSTANCE;
        final BitSet delimSet = new BitSet();
        for (final char separator : separators) {
            delimSet.set(separator);
        }
        final ParserCursor cursor = new ParserCursor(0, buf.length());
        final List<NameValuePair> list = new ArrayList<>();
        while (!cursor.atEnd()) {
            delimSet.set('=');
            final String name = tokenParser.parseToken(buf, cursor, delimSet);
            String value = null;
            if (!cursor.atEnd()) {
                final int delim = buf.charAt(cursor.getPos());
                cursor.updatePos(cursor.getPos() + 1);
                if (delim == '=') {
                    delimSet.clear('=');
                    value = tokenParser.parseToken(buf, cursor, delimSet);
                    if (!cursor.atEnd()) {
                        cursor.updatePos(cursor.getPos() + 1);
                    }
                }
            }
            if (!name.isEmpty()) {
                list.add(
                        new NameValuePair(
                                decodeFormFields(name, charset), decodeFormFields(value, charset)));
            }
        }
        return list;
    }

    static List<String> splitSegments(final CharSequence segments, final BitSet separators) {
        final ParserCursor cursor = new ParserCursor(0, segments.length());
        // Skip leading separator
        if (cursor.atEnd()) {
            return Collections.emptyList();
        }
        if (separators.get(segments.charAt(cursor.getPos()))) {
            cursor.updatePos(cursor.getPos() + 1);
        }
        final List<String> list = new ArrayList<>();
        final StringBuilder buf = new StringBuilder();
        for (; ; ) {
            if (cursor.atEnd()) {
                list.add(buf.toString());
                break;
            }
            final char current = segments.charAt(cursor.getPos());
            if (separators.get(current)) {
                list.add(buf.toString());
                buf.setLength(0);
            } else {
                buf.append(current);
            }
            cursor.updatePos(cursor.getPos() + 1);
        }
        return list;
    }

    static List<String> splitPathSegments(final CharSequence s) {
        return splitSegments(s, PATH_SEPARATORS);
    }

    /**
     * Returns a list of URI path segments.
     *
     * @param sequence URI path component.
     * @param charset parameter charset.
     * @return list of segments.
     */
    static List<String> parsePathSegments(final CharSequence sequence, final Charset charset) {
        Preconditions.checkNotNull(sequence, "Char sequence cannot be null.");
        final List<String> list = splitPathSegments(sequence);
        for (int i = 0; i < list.size(); i++) {
            list.set(
                    i,
                    urlDecode(
                            list.get(i),
                            charset != null ? charset : StandardCharsets.UTF_8,
                            false));
        }
        return list;
    }

    /**
     * Returns a string consisting of joint encoded path segments.
     *
     * @param segments the segments.
     * @param charset parameter charset.
     * @return URI path component
     */
    static String formatSegments(final Iterable<String> segments, final Charset charset) {
        Preconditions.checkNotNull(segments, "Segments cannot be null.");
        final StringBuilder result = new StringBuilder();
        for (final String segment : segments) {
            result.append(PATH_SEPARATOR).append(urlEncode(segment, charset, PATHSAFE, false));
        }
        return result.toString();
    }

    /**
     * Returns a String that is suitable for use as an {@code application/x-www-form-urlencoded}
     * list of parameters in an HTTP PUT or HTTP POST.
     *
     * @param parameters The parameters to include.
     * @param charset The encoding to use.
     * @return An {@code application/x-www-form-urlencoded} string
     */
    public static String format(
            final Iterable<? extends NameValuePair> parameters, final Charset charset) {
        return format(parameters, QP_SEP_A, charset);
    }

    /**
     * Returns a String that is suitable for use as an {@code application/x-www-form-urlencoded}
     * list of parameters in an HTTP PUT or HTTP POST.
     *
     * @param parameters The parameters to include.
     * @param parameterSeparator The parameter separator, by convention, {@code '&'} or {@code ';'}.
     * @param charset The encoding to use.
     * @return An {@code application/x-www-form-urlencoded} string
     */
    static String format(
            final Iterable<? extends NameValuePair> parameters,
            final char parameterSeparator,
            final Charset charset) {
        Preconditions.checkNotNull(parameters, "Parameters cannot be null.");
        final StringBuilder result = new StringBuilder();
        for (final NameValuePair parameter : parameters) {
            final String encodedName = encodeFormFields(parameter.getName(), charset);
            final String encodedValue = encodeFormFields(parameter.getValue(), charset);
            if (result.length() > 0) {
                result.append(parameterSeparator);
            }
            result.append(encodedName);
            if (encodedValue != null) {
                result.append(NAME_VALUE_SEPARATOR);
                result.append(encodedValue);
            }
        }
        return result.toString();
    }

    private static List<NameValuePair> createEmptyList() {
        return new ArrayList<>(0);
    }

    private static String urlEncode(
            final String content,
            final Charset charset,
            final BitSet safeChars,
            final boolean blankAsPlus) {
        if (content == null) {
            return null;
        }
        final StringBuilder buf = new StringBuilder();
        final ByteBuffer bb = charset.encode(content);
        while (bb.hasRemaining()) {
            final int b = bb.get() & 0xff;
            if (safeChars.get(b)) {
                buf.append((char) b);
            } else if (blankAsPlus && b == ' ') {
                buf.append('+');
            } else {
                buf.append("%");
                final char hex1 = Character.toUpperCase(Character.forDigit((b >> 4) & 0xF, RADIX));
                final char hex2 = Character.toUpperCase(Character.forDigit(b & 0xF, RADIX));
                buf.append(hex1);
                buf.append(hex2);
            }
        }
        return buf.toString();
    }

    /**
     * Decode/unescape a portion of a URL, to use with the query part ensure {@code plusAsBlank} is
     * true.
     *
     * @param content the portion to decode
     * @param charset the charset to use
     * @param plusAsBlank if {@code true}, then convert '+' to space (e.g. for www-url-form-encoded
     *     content), otherwise leave as is.
     * @return encoded string
     */
    private static String urlDecode(
            final String content, final Charset charset, final boolean plusAsBlank) {

        final ByteBuffer bb = ByteBuffer.allocate(content.length());
        final CharBuffer cb = CharBuffer.wrap(content);
        while (cb.hasRemaining()) {
            final char c = cb.get();
            if (c == '%' && cb.remaining() >= 2) {
                final char uc = cb.get();
                final char lc = cb.get();
                final int u = Character.digit(uc, 16);
                final int l = Character.digit(lc, 16);
                if (u != -1 && l != -1) {
                    bb.put((byte) ((u << 4) + l));
                } else {
                    bb.put((byte) '%');
                    bb.put((byte) uc);
                    bb.put((byte) lc);
                }
            } else if (plusAsBlank && c == '+') {
                bb.put((byte) ' ');
            } else {
                bb.put((byte) c);
            }
        }
        bb.flip();
        return charset.decode(bb).toString();
    }

    /**
     * Encode/escape www-url-form-encoded content.
     *
     * <p>Uses the {@link #URLENCODER} set of characters, rather than the {@link #UNRESERVED} set;
     * this is for compatibilty with previous releases, URLEncoder.encode() and most browsers.
     *
     * @param content the content to encode, will convert space to '+'
     * @param charset the charset to use
     * @return encoded string
     */
    private static String encodeFormFields(final String content, final Charset charset) {
        if (content == null) {
            return null;
        }
        return urlEncode(
                content, charset != null ? charset : StandardCharsets.UTF_8, URLENCODER, true);
    }

    /**
     * Decode/unescape www-url-form-encoded content.
     *
     * @param content the content to decode, will decode '+' as space
     * @param charset the charset to use
     * @return encoded string
     */
    private static String decodeFormFields(final String content, final Charset charset) {
        if (content == null) {
            return null;
        }
        return urlDecode(content, charset != null ? charset : StandardCharsets.UTF_8, true);
    }
}
