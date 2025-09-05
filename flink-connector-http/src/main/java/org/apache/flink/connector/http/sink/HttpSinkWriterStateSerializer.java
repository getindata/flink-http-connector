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

import org.apache.flink.connector.base.sink.writer.AsyncSinkWriterStateSerializer;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * An implementation of {@link AsyncSinkWriterStateSerializer} for {@link HttpSinkInternal} and its
 * {@link HttpSinkWriter}.
 */
public class HttpSinkWriterStateSerializer
        extends AsyncSinkWriterStateSerializer<HttpSinkRequestEntry> {

    @Override
    protected void serializeRequestToStream(HttpSinkRequestEntry s, DataOutputStream out)
            throws IOException {
        out.writeUTF(s.method);
        out.write(s.element);
    }

    @Override
    protected HttpSinkRequestEntry deserializeRequestFromStream(
            long requestSize, DataInputStream in) throws IOException {
        var method = in.readUTF();
        var bytes = new byte[(int) requestSize];
        in.read(bytes);
        return new HttpSinkRequestEntry(method, bytes);
    }

    @Override
    public int getVersion() {
        return 1;
    }
}
