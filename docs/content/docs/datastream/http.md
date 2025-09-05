---
title: HTTP 
weight: 3
type: docs
aliases:
  - /dev/connectors/http.html
  - /apis/streaming/connectors/http.html
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Apache HTTP Connector
The HTTP connector allows for pulling data from external system via HTTP methods and HTTP Sink that allows for sending data to external system via HTTP requests.

Note this connector was donated to Flink in [FLIP-532](https://cwiki.apache.org/confluence/display/FLINK/FLIP-532%3A+Donate+GetInData+HTTP+Connector+to+Flink).
Existing java applications built using the original repository will need to be recompiled to pick up the new flink package names.

The HTTP sink connector supports the Flink streaming API.

<!-- TOC -->
* [Apache HTTP Connector](#apache-http-connector)
  * [Working with HTTP sink Flink streaming API](#working-with-http-sink-flink-streaming-api)
    * [Sink Connector options](#sink-connector-options-)
    * [Batch submission mode](#batch-submission-mode)
    * [Single submission mode](#single-submission-mode)
    * [Http headers](#http-headers)
  * [TLS (more secure replacement for SSL) and mTLS support](#tls-more-secure-replacement-for-ssl-and-mtls-support)
  * [Basic Authentication](#basic-authentication)
  * [OIDC Bearer Authentication](#oidc-bearer-authentication)
    * [Restrictions at this time](#restrictions-at-this-time)
<!-- TOC -->
## Working with HTTP sink Flink streaming API

In order to change submission batch size use `flink.connector.http.sink.request.batch.size` property. For example:

### Sink Connector options 
These option are specified on the builder using the setProperty method.

| Option                                                  | Required | Description/Value                                                                                                                                                                                                                                |
|---------------------------------------------------------|----------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| connector                                               | required | Specify what connector to use. For HTTP Sink it should be set to _'http-sink'_.                                                                                                                                                                  |
| format                                                  | required | Specify what format to use.                                                                                                                                                                                                                      |
| url                                                     | required | The base URL that should be use for HTTP requests. For example _http://localhost:8080/client_.                                                                                                                                                   |
| insert-method                                           | optional | Specify which HTTP method to use in the request. The value should be set either to `POST` or `PUT`.                                                                                                                                              |
| sink.batch.max-size                                     | optional | Maximum number of elements that may be passed in a batch to be written downstream.                                                                                                                                                               |
| sink.requests.max-inflight                              | optional | The maximum number of in flight requests that may exist, if any more in flight requests need to be initiated once the maximum has been reached, then it will be blocked until some have completed.                                               |
| sink.requests.max-buffered                              | optional | Maximum number of buffered records before applying backpressure.                                                                                                                                                                                 |
| sink.flush-buffer.size                                  | optional | The maximum size of a batch of entries that may be sent to the HTTP endpoint measured in bytes.                                                                                                                                                  |
| sink.flush-buffer.timeout                               | optional | Threshold time in milliseconds for an element to be in a buffer before being flushed.                                                                                                                                                            |
| flink.connector.http.sink.request-callback                | optional | Specify which `HttpPostRequestCallback` implementation to use. By default, it is set to `slf4j-logger` corresponding to `Slf4jHttpPostRequestCallback`.                                                                                          |
| flink.connector.http.sink.error.code                      | optional | List of HTTP status codes that should be treated as errors by HTTP Sink, separated with comma.                                                                                                                                                   |
| flink.connector.http.sink.error.code.exclude              | optional | List of HTTP status codes that should be excluded from the `flink.connector.http.sink.error.code` list, separated with comma.                                                                                                                      |
| flink.connector.http.security.cert.server                 | optional | Path to trusted HTTP server certificate that should be add to connectors key store. More than one path can be specified using `,` as path delimiter.                                                                                             |
| flink.connector.http.security.cert.client                 | optional | Path to trusted certificate that should be used by connector's HTTP client for mTLS communication.                                                                                                                                               |
| flink.connector.http.security.key.client                  | optional | Path to trusted private key that should be used by connector's HTTP client for mTLS communication.                                                                                                                                               |
| flink.connector.http.security.cert.server.allowSelfSigned | optional | Accept untrusted certificates for TLS communication.                                                                                                                                                                                             |
| flink.connector.http.sink.request.timeout                 | optional | Sets HTTP request timeout in seconds. If not specified, the default value of 30 seconds will be used.                                                                                                                                            |
| flink.connector.http.sink.writer.thread-pool.size         | optional | Sets the size of pool thread for HTTP Sink request processing. Increasing this value would mean that more concurrent requests can be processed in the same time. If not specified, the default value of 1 thread will be used.                   |
| flink.connector.http.sink.writer.request.mode             | optional | Sets Http Sink request submission mode. Two modes are available to select, `single` and `batch` which is the default mode if option is not specified.                                                                                            |
| flink.connector.http.sink.request.batch.size              | optional | Applicable only for `flink.connector.http.sink.writer.request.mode = batch`. Sets number of individual events/requests that will be submitted as one HTTP request by HTTP sink. The default value is 500 which is same as HTTP Sink `maxBatchSize` |



### Batch submission mode

By default, batch size is set to 500 which is the same as Http Sink's `maxBatchSize` property and has value of 500.
The `maxBatchSize' property sets maximal number of events that will by buffered by Flink runtime before passing it to Http Sink for processing.

Streaming API:
```java
HttpSink.<String>builder()
      .setEndpointUrl("http://example.com/myendpoint")
      .setElementConverter(
          (s, _context) -> new HttpSinkRequestEntry("POST", s.getBytes(StandardCharsets.UTF_8)))
      .setProperty("http.sink.request.batch.size", "50")
      .build();
```

### Single submission mode
In this mode every processed event is submitted as individual HTTP POST/PUT request.

Streaming API:
```java
HttpSink.<String>builder()
      .setEndpointUrl("http://example.com/myendpoint")
      .setElementConverter(
          (s, _context) -> new HttpSinkRequestEntry("POST", s.getBytes(StandardCharsets.UTF_8)))
      .setProperty("http.sink.writer.request.mode", "single")
      .build();
```
### Http headers
It is possible to set HTTP headers that will be added to HTTP request send by sink connector.
Headers are defined via property key `flink.connector.http.sink.header.HEADER_NAME = header value` for example:
`flink.connector.http.sink.header.X-Content-Type-Options = nosniff`.
Properties can be set via Sink builder or Property object:
```java
HttpSink.<String>builder()
      .setEndpointUrl("http://example.com/myendpoint")
      .setElementConverter(
          (s, _context) -> new HttpSinkRequestEntry("POST", s.getBytes(StandardCharsets.UTF_8)))
      .setProperty("http.sink.header.X-Content-Type-Options", "nosniff")
      .build();
```
or

```java
Properties properties = Properties();
properties.setProperty("http.sink.header.X-Content-Type-Options", "nosniff");

HttpSink.<String>builder()
      .setEndpointUrl("http://example.com/myendpoint")
      .setElementConverter(
          (s, _context) -> new HttpSinkRequestEntry("POST", s.getBytes(StandardCharsets.UTF_8)))
      .setProperties(properties)
      .build();
```


## TLS (more secure replacement for SSL) and mTLS support

Both Http Sink and Lookup Source connectors support HTTPS communication using TLS 1.2 and mTLS.
To enable Https communication simply use `https` protocol in endpoint's URL.

To specify certificate(s) to be used by the server, use `flink.connector.http.security.cert.server` connector property;
the value is a comma separated list of paths to certificate(s), for example you can use your organization's CA
Root certificate, or a self-signed certificate.

Note that if there are no security properties for a `https` url then, the JVMs default certificates are
used - allowing use of globally recognized CAs without the need for configuration.

You can also configure the connector to use mTLS. For this simply use `flink.connector.http.security.cert.client`
and `flink.connector.http.security.key.client` connector properties to specify paths to the certificate and
private key. The key MUST be in `PCKS8` format. Both PEM and DER keys are
allowed.

All properties can be set via Sink's builder `.setProperty(...)` method or through Sink and Source table DDL.

For non production environments it is sometimes necessary to use Https connection and accept all certificates.
In this special case, you can configure connector to trust all certificates without adding them to keystore.
To enable this option use `flink.connector.http.security.cert.server.allowSelfSigned` property setting its value to `true`.

## Basic Authentication
The connector supports Basic Authentication using a HTTP `Authorization` header.
The header value can be set via properties, similarly as for other headers. The connector converts the passed value to Base64 and uses it for the request.
If the used value starts with the prefix `Basic`, or `flink.connector.http.source.lookup.use-raw-authorization-header`
is set to `'true'`, it will be used as header value as is, without any extra modification.

## OIDC Bearer Authentication
The connector supports Bearer Authentication using a HTTP `Authorization` header. The [OAuth 2.0 rcf](https://datatracker.ietf.org/doc/html/rfc6749) mentions [Obtaining Authorization](https://datatracker.ietf.org/doc/html/rfc6749#section-4)
and an authorization grant. OIDC makes use of this [authorisation grant](https://datatracker.ietf.org/doc/html/rfc6749#section-1.3) in a [Token Request](https://openid.net/specs/openid-connect-core-1_0.html#TokenRequest) by including a [OAuth grant type](https://oauth.net/2/grant-types/) and associated properties, the response is the [token response](https://openid.net/specs/openid-connect-core-1_0.html#TokenResponse).

If you want to use this authorization then you should supply the `Token Request` body in `application/x-www-form-urlencoded` encoding
in configuration property `flink.connector.http.security.oidc.token.request`. See [grant extension](https://datatracker.ietf.org/doc/html/rfc6749#section-4.5) for
an example of a customised grant type token request. The supplied `token request` will be issued to the
[token end point](https://datatracker.ietf.org/doc/html/rfc6749#section-3.2), whose url should be supplied in configuration property
`flink.connector.http.security.oidc.token.endpoint.url`. The returned `access token` is then cached and used for subsequent requests; if the token has expired then
a new one is requested. There is a property `flink.connector.http.security.oidc.token.expiry.reduction`, that defaults to 1 second; new tokens will
be requested if the current time is later than the cached token expiry time minus `flink.connector.http.security.oidc.token.expiry.reduction`.

### Restrictions at this time
* No authentication is applied to the token request.
* The processing does not use the refresh token if it present.
  {{< top >}}
