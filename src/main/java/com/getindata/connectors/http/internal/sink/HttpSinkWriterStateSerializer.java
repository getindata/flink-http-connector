package com.getindata.connectors.http.internal.sink;

import org.apache.flink.connector.base.sink.writer.AsyncSinkWriterStateSerializer;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * An implementation of {@link AsyncSinkWriterStateSerializer} for {@link HttpSinkInternal} and its {@link HttpSinkWriter}.
 */
public class HttpSinkWriterStateSerializer extends AsyncSinkWriterStateSerializer<HttpSinkRequestEntry> {
  @Override
  protected void serializeRequestToStream(HttpSinkRequestEntry s, DataOutputStream out) throws IOException {
    out.writeUTF(s.method);
    out.writeUTF(s.contentType);
    out.write(s.element);
  }

  @Override
  protected HttpSinkRequestEntry deserializeRequestFromStream(long requestSize, DataInputStream in) throws IOException {
    var method = in.readUTF();
    var contentType = in.readUTF();
    var bytes = new byte[(int) requestSize];
    in.read(bytes);
    return new HttpSinkRequestEntry(method, contentType, bytes);
  }

  @Override
  public int getVersion() {
    return 1;
  }
}
