# flink-http-connector

[![Maven Central](https://img.shields.io/maven-central/v/com.getindata/flink-http-connector)](https://mvnrepository.com/artifact/com.getindata/flink-http-connector)
[![javadoc](https://javadoc.io/badge2/com.getindata/flink-http-connector/javadoc.svg)](https://javadoc.io/doc/com.getindata/flink-http-connector) 

The HTTP TableLookup connector that allows for pulling data from external system via HTTP GET method and HTTP Sink that allows for sending data to external system via HTTP requests.

**Note**: The `main` branch may be in an *unstable or even broken state* during development.
Please use [releases](https://github.com/getindata/flink-http-connector/releases) instead of the `main` branch in order to get a stable set of binaries.

The goal for HTTP TableLookup connector was to use it in Flink SQL statement as a standard table that can be later joined with other stream using pure SQL Flink.
 
Currently, HTTP source connector supports only Lookup Joins (TableLookup) [1] in Table/SQL API.
`HttpSink` supports both Streaming API (when using [HttpSink](src/main/java/com/getindata/connectors/http/internal/sink/HttpSink.java) built using [HttpSinkBuilder](src/main/java/com/getindata/connectors/http/internal/sink/HttpSinkBuilder.java)) and the Table API (using connector created in [HttpDynamicTableSinkFactory](src/main/java/com/getindata/connectors/http/internal/table/HttpDynamicTableSinkFactory.java)). 

## Updating the connector
In case of updating http-connector please see [Breaking changes](#breaking-changes) section.

## Prerequisites
* Java 11
* Maven 3
* Flink 1.18+. Recommended Flink 1.20.* 



## Runtime dependencies
This connector has few Flink's runtime dependencies, that are expected to be provided.
* `org.apache.flink.flink-java`
* `org.apache.flink.flink-clients`
* `org.apache.flink.flink-connector-base`

## Installation

In order to use the `flink-http-connector` the following dependencies are required for both projects using a build automation tool (such as Maven or SBT) and SQL Client with SQL JAR bundles. For build automation tool reference, look into Maven Central: [https://mvnrepository.com/artifact/com.getindata/flink-http-connector](https://mvnrepository.com/artifact/com.getindata/flink-http-connector).

## Documentation

You can read the official JavaDoc documentation of the latest release at [https://javadoc.io/doc/com.getindata/flink-http-connector](https://javadoc.io/doc/com.getindata/flink-http-connector).

## Usage

### HTTP TableLookup Source
Flink SQL table definition:

Enrichment Lookup Table
```roomsql
CREATE TABLE Customers (
    id STRING,
    id2 STRING,
    msg STRING,
    uuid STRING,
    details ROW<
      isActive BOOLEAN,
      nestedDetails ROW<
        balance STRING
      >
    >
) WITH (
'connector' = 'rest-lookup',
'format' = 'json',
'url' = 'http://localhost:8080/client',
'asyncPolling' = 'true'
)
```

Data Source Table
```roomsql
CREATE TABLE Orders (
    id STRING,
    id2 STRING,
    proc_time AS PROCTIME()
) WITH (
'connector' = 'datagen',
'rows-per-second' = '1',
'fields.id.kind' = 'sequence',
'fields.id.start' = '1',
'fields.id.end' = '120',
'fields.id2.kind' = 'sequence',
'fields.id2.start' = '2',
'fields.id2.end' = '120'
);
```

Using _Customers_ table in Flink SQL Lookup Join with _Orders_ table:

```roomsql
SELECT o.id, o.id2, c.msg, c.uuid, c.isActive, c.balance FROM Orders AS o 
JOIN Customers FOR SYSTEM_TIME AS OF o.proc_time AS c ON o.id = c.id AND o.id2 = c.id2
```

The columns and their values used for JOIN `ON` condition will be used as HTTP GET parameters where the column name will be used as a request parameter name. 

For Example: 
``
http://localhost:8080/client/service?id=1&uuid=2
``

Or for REST POST method they will be converted to Json and used as request body. In this case, json request body will look like this:
```json
{
    "id": "1",
    "uuid": "2"
}
```

#### Http headers
It is possible to set HTTP headers that will be added to HTTP request send by lookup source connector.
Headers are defined via property key `gid.connector.http.source.lookup.header.HEADER_NAME = header value` for example:
`gid.connector.http.source.lookup.header.X-Content-Type-Options = nosniff`.

Headers can be set using http lookup source table DDL. In example below, HTTP request done for `http-lookup` table will contain three headers:
- `Origin`
- `X-Content-Type-Options`
- `Content-Type`

```roomsql
CREATE TABLE http-lookup (
  id bigint,
  some_field string
) WITH (
  'connector' = 'rest-lookup',
  'format' = 'json',
  'url' = 'http://localhost:8080/client',
  'asyncPolling' = 'true',
  'gid.connector.http.source.lookup.header.Origin' = '*',
  'gid.connector.http.source.lookup.header.X-Content-Type-Options' = 'nosniff',
  'gid.connector.http.source.lookup.header.Content-Type' = 'application/json'
)
```

#### Custom REST query
Http Lookup Source builds queries out of `JOIN` clauses. One can customize how those queries are built by implementing
[LookupQueryCreator](src/main/java/com/getindata/connectors/http/LookupQueryCreator.java) and
[LookupQueryCreatorFactory](src/main/java/com/getindata/connectors/http/LookupQueryCreatorFactory.java) interfaces.
Custom implementations of `LookupQueryCreatorFactory` can be registered along other factories in
`resources/META-INF.services/org.apache.flink.table.factories.Factory` file and then referenced by their identifiers in
the Http Lookup Source DDL property field `gid.connector.http.source.lookup.query-creator`.

A default implementation that builds an "ordinary" GET query, i.e. adds `?joinColumn1=value1&joinColumn2=value2&...`
to the URI of the endpoint,

For body based queries such as POST/PUT requests, the 
([GenericGetQueryCreator](src/main/java/com/getindata/connectors/http/internal/table/lookup/querycreators/GenericGetQueryCreator.java))
is provided as a default query creator. This implementation uses Flink's [json-format](https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/table/formats/json/)  to convert RowData object into Json String.

The `GenericGetQueryCreator` allows for using custom formats that will perform serialization to Json. Thanks to this, users can create their own logic for converting RowData to Json Strings suitable for their HTTP endpoints and use this logic as custom format
with HTTP Lookup connector and SQL queries.
To create a custom format user has to implement Flink's `SerializationSchema` and `SerializationFormatFactory` interfaces and register custom format factory along other factories in
`resources/META-INF.services/org.apache.flink.table.factories.Factory` file. This is common Flink mechanism for providing custom implementations for various factories.

In order to use custom format, user has to specify option `'lookup-request.format' = 'customFormatName'`, where `customFormatName` is the identifier of custom format factory.

Additionally, it is possible to pass query format options from table's DDL.
This can be done by using option like so: `'lookup-request.format.customFormatName.customFormatProperty' = 'propertyValue'`, for example
`'lookup-request.format.customFormatName.fail-on-missing-field' = 'true'`.

It is important that `customFormatName` part match `SerializationFormatFactory` identifier used for custom format implementation.
In this case, the `fail-on-missing-field` will be passed to `SerializationFormatFactory::createEncodingFormat(
DynamicTableFactory.Context context, ReadableConfig formatOptions)` method in `ReadableConfig` object.

With default configuration, Flink-Json format is used for `GenericGetQueryCreator`, all options defined in [json-format](https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/table/formats/json/)
can be passed through table DDL. For example `'lookup-request.format.json.fail-on-missing-field' = 'true'`. In this case, format identifier is `json`.

#### Timeouts
Lookup Source is guarded by two timeout timers. First one is specified by Flink's AsyncIO operator that executes `AsyncTableFunction`.
The default value of this timer is set to 3 minutes and can be changed via `table.exec.async-lookup.timeout` [option](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/config/#table-exec-async-lookup-timeout).

The second one is set per individual HTTP requests by HTTP client. Its default value is set currently to 30 seconds and can be changed via `gid.connector.http.source.lookup.request.timeout` option. 

Flink's current implementation of `AsyncTableFunction` does not allow specifying custom logic for handling Flink AsyncIO timeouts as it is for Java API.
Because of that, if AsyncIO timer passes, Flink will throw TimeoutException which will cause job restart.

#### Retries (Lookup source)
Lookup source handles auto-retries for two scenarios:
1. IOException occurs (e.g. temporary network outage)
2. The response contains a HTTP error code that indicates a retriable error. These codes are defined in the table configuration (see `gid.connector.http.source.lookup.retry-codes`).
Retries are executed silently, without restarting the job. After reaching max retries attempts (per request) operation will fail and restart job.

Notice that HTTP codes are categorized into into 3 groups:
- successful responses - response is returned immediately for further processing
- temporary errors - request will be retried up to the retry limit
- error responses - unexpected responses are not retried and will fail the job. Any HTTP error code which is not configured as successful or temporary error is treated as an unretriable error.

##### Retry strategy
User can choose retry strategy type for source table:
- fixed-delay - http request will be re-sent after specified delay.
- exponential-delay - request will be re-sent with exponential backoff strategy, limited by `lookup.max-retries` attempts. The delay for each retry is calculated as the previous attempt's delay multiplied by the backoff multiplier (parameter `gid.connector.http.source.lookup.retry-strategy.exponential-delay.backoff-multiplier`) up to `gid.connector.http.source.lookup.retry-strategy.exponential-delay.max-backoff`. The initial delay value is defined in the table configuration as `gid.connector.http.source.lookup.retry-strategy.exponential-delay.initial-backoff`.


#### Lookup multiple results

Typically, join can return zero, one or more results. What is more, there are lots of possible REST API designs and
pagination methods. Currently, the connector supports only two simple approaches (`gid.connector.http.source.lookup.result-type`):

- `single-value` - REST API returns single object.
- `array` - REST API returns array of objects. Pagination is not supported yet.

Please be informed that the mechanism will be enhanced in the future. See [HTTP-118](https://github.com/getindata/flink-http-connector/issues/118).

### HTTP Sink
The following example shows the minimum Table API example to create a [HttpDynamicSink](src/main/java/com/getindata/connectors/http/internal/table/HttpDynamicSink.java) that writes JSON values to an HTTP endpoint using POST method, assuming Flink has JAR of [JSON serializer](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/connectors/table/formats/json/) installed:

```roomsql
CREATE TABLE http (
  id bigint,
  some_field string
) WITH (
  'connector' = 'http-sink',
  'url' = 'http://example.com/myendpoint',
  'format' = 'json'
)
```

Then use `INSERT` SQL statement to send data to your HTTP endpoint:

```roomsql
INSERT INTO http VALUES (1, 'Ninette'), (2, 'Hedy')
```

Due to the fact that `HttpSink` sends bytes inside HTTP request's body, one can easily swap `'format' = 'json'` for some other [format](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/connectors/table/formats/overview/). 

Other examples of usage of the Table API can be found in [some tests](src/test/java/com/getindata/connectors/http/table/HttpDynamicSinkInsertTest.java).

### Request submission
Starting from version 0.10 HTTP Sink by default submits events in batch. Before version 0.10 the default and only submission type was `single`.
This is a breaking compatibility change.

The submission mode can be changed using `gid.connector.http.sink.writer.request.mode` property using `single` or `batch` as property value.

#### Batch submission mode
In batch mode, a number of events (processed elements) will be batched and submitted in one HTTP request.
In this mode, HTTP PUT/POST request's body contains a Json array, where every element of this array represents
individual event.

An example of Http Sink batch request body containing data for three events:
```json
[
  {
    "id": 1,
    "first_name": "Ninette",
    "last_name": "Clee",
    "gender": "Female",
    "stock": "CDZI",
    "currency": "RUB",
    "tx_date": "2021-08-24 15:22:59"
  },
  {
    "id": 2,
    "first_name": "Rob",
    "last_name": "Zombie",
    "gender": "Male",
    "stock": "DGICA",
    "currency": "GBP",
    "tx_date": "2021-10-25 20:53:54"
  },
  {
    "id": 3,
    "first_name": "Adam",
    "last_name": "Jones",
    "gender": "Male",
    "stock": "DGICA",
    "currency": "PLN",
    "tx_date": "2021-10-26 20:53:54"
  }
]
```

By default, batch size is set to 500 which is the same as Http Sink's `maxBatchSize` property and has value of 500. 
The `maxBatchSize' property sets maximal number of events that will by buffered by Flink runtime before passing it to Http Sink for processing.

In order to change submission batch size use `gid.connector.http.sink.request.batch.size` property. For example:

Streaming API:
```java
HttpSink.<String>builder()
      .setEndpointUrl("http://example.com/myendpoint")
      .setElementConverter(
          (s, _context) -> new HttpSinkRequestEntry("POST", s.getBytes(StandardCharsets.UTF_8)))
      .setProperty("gid.connector.http.sink.request.batch.size", "50")
      .build();
```
SQL:
```roomsql
CREATE TABLE http (
  id bigint,
  some_field string
) WITH (
  'connector' = 'http-sink',
  'url' = 'http://example.com/myendpoint',
  'format' = 'json',
  'gid.connector.http.sink.request.batch.size' = '50'
)
```

#### Single submission mode
In this mode every processed event is submitted as individual HTTP POST/PUT request. 

Streaming API:
```java
HttpSink.<String>builder()
      .setEndpointUrl("http://example.com/myendpoint")
      .setElementConverter(
          (s, _context) -> new HttpSinkRequestEntry("POST", s.getBytes(StandardCharsets.UTF_8)))
      .setProperty("gid.connector.http.sink.writer.request.mode", "single")
      .build();
```
SQL:
```roomsql
CREATE TABLE http (
  id bigint,
  some_field string
) WITH (
  'connector' = 'http-sink',
  'url' = 'http://example.com/myendpoint',
  'format' = 'json',
  'gid.connector.http.sink.writer.request.mode' = 'single'
)
```

#### Http headers
It is possible to set HTTP headers that will be added to HTTP request send by sink connector.
Headers are defined via property key `gid.connector.http.sink.header.HEADER_NAME = header value` for example:
`gid.connector.http.sink.header.X-Content-Type-Options = nosniff`.
Properties can be set via Sink builder or Property object:
```java
HttpSink.<String>builder()
      .setEndpointUrl("http://example.com/myendpoint")
      .setElementConverter(
          (s, _context) -> new HttpSinkRequestEntry("POST", s.getBytes(StandardCharsets.UTF_8)))
      .setProperty("gid.connector.http.sink.header.X-Content-Type-Options", "nosniff")
      .build();
```
or

```java
Properties properties = Properties();
properties.setProperty("gid.connector.http.sink.header.X-Content-Type-Options", "nosniff");

HttpSink.<String>builder()
      .setEndpointUrl("http://example.com/myendpoint")
      .setElementConverter(
          (s, _context) -> new HttpSinkRequestEntry("POST", s.getBytes(StandardCharsets.UTF_8)))
      .setProperties(properties)
      .build();
```

In Table/SQL API, headers can be set using http sink table DDL. In example below, HTTP request done for `http` table will contain three headers:
- `Origin`
- `X-Content-Type-Options`
- `Content-Type`

```roomsql
CREATE TABLE http (
  id bigint,
  some_field string
) WITH (
  'connector' = 'http-sink',
  'url' = 'http://example.com/myendpoint',
  'format' = 'json',
  'gid.connector.http.sink.header.Origin' = '*',
  'gid.connector.http.sink.header.X-Content-Type-Options' = 'nosniff',
  'gid.connector.http.sink.header.Content-Type' = 'application/json'
)
```

Note that when using OIDC, it adds an `Authentication` header with the bearer token; this will override 
an existing `Authorization` header specified in configuration.

#### Custom request/response callback

- Http Sink processes responses that it gets from the HTTP endpoint along their respective requests. One can customize the
behaviour of the additional stage of processing done by Table API Sink by implementing
[HttpPostRequestCallback](src/main/java/com/getindata/connectors/http/HttpPostRequestCallback.java) and
[HttpPostRequestCallbackFactory](src/main/java/com/getindata/connectors/http/HttpPostRequestCallbackFactory.java)
interfaces. Custom implementations of `HttpPostRequestCallbackFactory<HttpRequest>` can be registered along other factories in
`resources/META-INF/services/org.apache.flink.table.factories.Factory` file and then referenced by their identifiers in
the HttpSink DDL property field `gid.connector.http.sink.request-callback`.

   For example, one can create a class `CustomHttpSinkPostRequestCallbackFactory` with a unique identifier, say `rest-sink-logger`,
that implements interface `HttpPostRequestCallbackFactory<HttpRequest>` to create a new instance of a custom callback
`CustomHttpSinkPostRequestCallback`. This factory can be registered along other factories by appending the fully-qualified name 
of class `CustomHttpSinkPostRequestCallbackFactory` in `resources/META-INF/services/org.apache.flink.table.factories.Factory` file 
and then reference identifier `rest-sink-logger` in the HttpSink DDL property field `gid.connector.http.sink.request-callback`.

  A default implementation that logs those pairs as *INFO* level logs using Slf4j
([Slf4jHttpPostRequestCallback](src/main/java/com/getindata/connectors/http/internal/table/sink/Slf4jHttpPostRequestCallback.java))
is provided.


- Http Lookup Source processes responses that it gets from the HTTP endpoint along their respective requests. One can customize the
behaviour of the additional stage of processing done by Table Function API by implementing
[HttpPostRequestCallback](src/main/java/com/getindata/connectors/http/HttpPostRequestCallback.java) and
[HttpPostRequestCallbackFactory](src/main/java/com/getindata/connectors/http/HttpPostRequestCallbackFactory.java)
interfaces. 
 
   For example, one can create a class `CustomHttpLookupPostRequestCallbackFactory` with a unique identifier, say `rest-lookup-logger`,
that implements interface `HttpPostRequestCallbackFactory<HttpLookupSourceRequestEntry>` to create a new instance of a custom callback
`CustomHttpLookupPostRequestCallback`. This factory can be registered along other factories by appending the fully-qualified name
of class `CustomHttpLookupPostRequestCallbackFactory` in `resources/META-INF/services/org.apache.flink.table.factories.Factory` file 
and then reference identifier `rest-lookup-logger` in the HTTP lookup DDL property field `gid.connector.http.source.lookup.request-callback`.

   A default implementation that logs those pairs as *INFO* level logs using Slf4j
([Slf4JHttpLookupPostRequestCallback](src/main/java/com/getindata/connectors/http/internal/table/lookup/Slf4JHttpLookupPostRequestCallback.java))
is provided.


## HTTP status code handler
### Sink table
You can configure a list of HTTP status codes that should be treated as errors for HTTP sink table.
By default all 400 and 500 response codes will be interpreted as error code.

This behavior can be changed by using below properties in table definition (DDL) or passing it via `setProperty' method from Sink's builder. The property name are:
- `gid.connector.http.sink.error.code` used to defined HTTP status code value that should be treated as error for example 404.
Many status codes can be defined in one value, where each code should be separated with comma, for example:
`401, 402, 403`. User can use this property also to define a type code mask. In that case, all codes from given HTTP response type will be treated as errors.
An example of such a mask would be `3XX, 4XX, 5XX`. In this case, all 300s, 400s and 500s status codes will be treated as errors.
- `gid.connector.http.sink.error.code.exclude` used to exclude a HTTP code from error list.
   Many status codes can be defined in one value, where each code should be separated with comma, for example:
  `401, 402, 403`. In this example, codes 401, 402 and 403 would not be interpreted as error codes.

### Source table
The source table categorizes HTTP responses into three groups based on status codes:
- Retry codes (`gid.connector.http.source.lookup.retry-codes`):
Responses in this group indicate a temporary issue (it can be e.g., HTTP 503 Service Unavailable). When such a response is received, the request should be retried.
- Success codes (`gid.connector.http.source.lookup.success-codes`):
These are expected responses that should be processed by table function.
- Ignored responses (`gid.connector.http.source.lookup.ignored-response-codes`):
Successful response, but its content will be ignored. For example, an HTTP 404 Not Found response is valid and indicates that the requested item does not exist, so its content can be ignored.
- Error codes: 
Any response code that is not classified as a retry or success code falls into this category. Receiving such a response will result in a job failure.


Above parameters support whitelisting and blacklisting. A sample configuration may look like this:
`2XX,404,!203` - meaning all codes from group 2XX (200-299), with 404 and without 203 ('!' character). Group blacklisting e.g. !2XX is not supported.

The same format is used in parameter `gid.connector.http.source.lookup.retry-codes`.

Example with explanation:
```roomsql
CREATE TABLE [...]
WITH (
  [...],
  'gid.connector.http.source.lookup.success-codes' = '2XX',
  'gid.connector.http.source.lookup.retry-codes' = '5XX,!501,!505,!506',
  'gid.connector.http.source.lookup.ignored-response-codes' = '404'
)
```
All 200s codes and 404 are considered as successful (`success-codes`, `ignored-response-codes`). These responses won't cause retry or job failure. 404 response is listed in `ignored-response-codes` parameter, what means content body will be ignored. Http with 404 code will produce just empty record.
When server returns response with 500s code except 501, 505 and 506 then connector will re-send request based on configuration in `gid.connector.http.source.lookup.retry-strategy` parameters. By default it's fixed-delay with 1 second delay, up to 3 times per request (parameter `lookup.max-retries`). After exceeding max-retries limit the job will fail.
A response with any other code than specified in params `success-codes` and `retry-codes` e.g. 400, 505, 301 will cause job failure.


```roomsql
CREATE TABLE [...]
WITH (
  [...],
  'gid.connector.http.source.lookup.success-codes' = '2XX',
  'gid.connector.http.source.lookup.retry-codes' = '',
  'gid.connector.http.source.lookup.ignored-response-codes' = '1XX,3XX,4XX,5XX'
)
```
In this configuration, all HTTP responses are considered successful because the sets `success-codes` and `ignored-response-codes` together cover all possible status codes. As a result, no retries will be triggered based on HTTP response codes. However, only responses with status code 200 will be parsed and processed by the Flink operator. Responses with status codes in the 1xx, 3xx, 4xx, and 5xx ranges are classified under `ignored-response-codes`.
Note that retries remain enabled and will still occur on IOException.
To disable retries, set `'lookup.max-retries' = '0'`.



## TLS (more secure replacement for SSL) and mTLS support

Both Http Sink and Lookup Source connectors support HTTPS communication using TLS 1.2 and mTLS.
To enable Https communication simply use `https` protocol in endpoint's URL.

To specify certificate(s) to be used by the server, use `gid.connector.http.security.cert.server` connector property;
the value is a comma separated list of paths to certificate(s), for example you can use your organization's CA
Root certificate, or a self-signed certificate.

Note that if there are no security properties for a `https` url then, the JVMs default certificates are
used - allowing use of globally recognized CAs without the need for configuration.

You can also configure the connector to use mTLS. For this simply use `gid.connector.http.security.cert.client`
and `gid.connector.http.security.key.client` connector properties to specify paths to the certificate and
private key. The key MUST be in `PCKS8` format. Both PEM and DER keys are
allowed.

All properties can be set via Sink's builder `.setProperty(...)` method or through Sink and Source table DDL.

For non production environments it is sometimes necessary to use Https connection and accept all certificates.
In this special case, you can configure connector to trust all certificates without adding them to keystore.
To enable this option use `gid.connector.http.security.cert.server.allowSelfSigned` property setting its value to `true`.

## Basic Authentication
The connector supports Basic Authentication using a HTTP `Authorization` header.
The header value can be set via properties, similarly as for other headers. The connector converts the passed value to Base64 and uses it for the request.
If the used value starts with the prefix `Basic`, or `gid.connector.http.source.lookup.use-raw-authorization-header`
is set to `'true'`, it will be used as header value as is, without any extra modification.

## OIDC Bearer Authentication
The connector supports Bearer Authentication using a HTTP `Authorization` header. The [OAuth 2.0 rcf](https://datatracker.ietf.org/doc/html/rfc6749) mentions [Obtaining Authorization](https://datatracker.ietf.org/doc/html/rfc6749#section-4)
and an authorization grant. OIDC makes use of this [authorisation grant](https://datatracker.ietf.org/doc/html/rfc6749#section-1.3) in a [Token Request](https://openid.net/specs/openid-connect-core-1_0.html#TokenRequest) by including a [OAuth grant type](https://oauth.net/2/grant-types/) and associated properties, the response is the [token response](https://openid.net/specs/openid-connect-core-1_0.html#TokenResponse). 

If you want to use this authorization then you should supply the `Token Request` body in `application/x-www-form-urlencoded` encoding
in configuration property `gid.connector.http.security.oidc.token.request`. See [grant extension](https://datatracker.ietf.org/doc/html/rfc6749#section-4.5) for
an example of a customised grant type token request. The supplied `token request` will be issued to the
[token end point](https://datatracker.ietf.org/doc/html/rfc6749#section-3.2), whose url should be supplied in configuration property 
`gid.connector.http.security.oidc.token.endpoint.url`. The returned `access token` is then cached and used for subsequent requests; if the token has expired then
 a new one is requested. There is a property `gid.connector.http.security.oidc.token.expiry.reduction`, that defaults to 1 second; new tokens will
be requested if the current time is later than the cached token expiry time minus `gid.connector.http.security.oidc.token.expiry.reduction`.

### Restrictions at this time
* No authentication is applied to the token request. 
* The processing does not use the refresh token if it present. 

## Table API Connector Options
### HTTP TableLookup Source

| Option                                                                               | Required | Description/Value                                                                                                                                                                                                                                                                                                                                                 |
|--------------------------------------------------------------------------------------|----------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| connector                                                                            | required | The Value should be set to _rest-lookup_                                                                                                                                                                                                                                                                                                                          |
| format                                                                               | required | Flink's format name that should be used to decode REST response, Use `json` for a typical REST endpoint.                                                                                                                                                                                                                                                          |
| url                                                                                  | required | The base URL that should be use for GET requests. For example _http://localhost:8080/client_                                                                                                                                                                                                                                                                      |
| asyncPolling                                                                         | optional | true/false - determines whether Async Polling should be used. Mechanism is based on Flink's Async I/O.                                                                                                                                                                                                                                                            |
| lookup-method                                                                        | optional | GET/POST/PUT (and any other) - determines what REST method should be used for lookup REST query. If not specified, `GET` method will be used.                                                                                                                                                                                                                     |
| lookup.cache                                                                         | optional | Enum possible values: `NONE`, `PARTIAL`. The cache strategy for the lookup table. Currently supports `NONE` (no caching) and `PARTIAL` (caching entries on lookup operation in external API).                                                                                                                                                                     |
| lookup.partial-cache.max-rows                                                        | optional | The max number of rows of lookup cache, over this value, the oldest rows will be expired. `lookup.cache` must be set to `PARTIAL` to use this option. See the following <a href="#lookup-cache">Lookup Cache</a> section for more details.                                                                                                                        |
| lookup.partial-cache.expire-after-write                                              | optional | The max time to live for each rows in lookup cache after writing into the cache. Specify as a [Duration](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/deployment/config/#duration).  `lookup.cache` must be set to `PARTIAL` to use this option. See the following <a href="#lookup-cache">Lookup Cache</a> section for more details.          |
| lookup.partial-cache.expire-after-access                                             | optional | The max time to live for each rows in lookup cache after accessing the entry in the cache. Specify as a [Duration](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/deployment/config/#duration). `lookup.cache` must be set to `PARTIAL` to use this option. See the following <a href="#lookup-cache">Lookup Cache</a> section for more details. |
| lookup.partial-cache.cache-missing-key                                               | optional | This is a boolean that defaults to true. Whether to store an empty value into the cache if the lookup key doesn't match any rows in the table. `lookup.cache` must be set to `PARTIAL` to use this option. See the following <a href="#lookup-cache">Lookup Cache</a> section for more details.                                                                   |
| lookup.max-retries                                                                   | optional | The max retry times if the lookup failed; default is 3. See the following <a href="#lookup-cache">Lookup Cache</a> section for more detail. Set value 0 to disable retries.                                                                                                                                                                                       |
| gid.connector.http.lookup.error.code                                                 | optional | List of HTTP status codes that should be treated as errors by HTTP Source, separated with comma.                                                                                                                                                                                                                                                                  |
| gid.connector.http.lookup.error.code.exclude                                         | optional | List of HTTP status codes that should be excluded from the `gid.connector.http.lookup.error.code` list, separated with comma.                                                                                                                                                                                                                                     |
| gid.connector.http.security.cert.server                                              | optional | Comma separated paths to trusted HTTP server certificates that should be added to the connectors trust store.                                                                                                                                                                                                                                                     |
| gid.connector.http.security.cert.client                                              | optional | Path to trusted certificate that should be used by connector's HTTP client for mTLS communication.                                                                                                                                                                                                                                                                |
| gid.connector.http.security.key.client                                               | optional | Path to trusted private key that should be used by connector's HTTP client for mTLS communication.                                                                                                                                                                                                                                                                |
| gid.connector.http.security.cert.server.allowSelfSigned                              | optional | Accept untrusted certificates for TLS communication.                                                                                                                                                                                                                                                                                                              |
| gid.connector.http.security.oidc.token.request                                       | optional | OIDC `Token Request` body in `application/x-www-form-urlencoded` encoding                                                                                                                                                                                                                                                                                         |
| gid.connector.http.security.oidc.token.endpoint.url                                  | optional | OIDC `Token Endpoint` url, to which the token request will be issued                                                                                                                                                                                                                                                                                              |
| gid.connector.http.security.oidc.token.expiry.reduction                              | optional | OIDC tokens will be requested if the current time is later than the cached token expiry time minus this value.                                                                                                                                                                                                                                                    |
| gid.connector.http.source.lookup.request.timeout                                     | optional | Sets HTTP request timeout in seconds. If not specified, the default value of 30 seconds will be used.                                                                                                                                                                                                                                                             |
| gid.connector.http.source.lookup.request.thread-pool.size                            | optional | Sets the size of pool thread for HTTP lookup request processing. Increasing this value would mean that more concurrent requests can be processed in the same time. If not specified, the default value of 8 threads will be used.                                                                                                                                 |
| gid.connector.http.source.lookup.response.thread-pool.size                           | optional | Sets the size of pool thread for HTTP lookup response processing. Increasing this value would mean that more concurrent requests can be processed in the same time. If not specified, the default value of 4 threads will be used.                                                                                                                                |
| gid.connector.http.source.lookup.use-raw-authorization-header                        | optional | If set to `'true'`, uses the raw value set for the `Authorization` header, without transformation for Basic Authentication (base64, addition of "Basic " prefix). If not specified, defaults to `'false'`.                                                                                                                                                        |
| gid.connector.http.source.lookup.request-callback                                    | optional | Specify which `HttpLookupPostRequestCallback` implementation to use. By default, it is set to `slf4j-lookup-logger` corresponding to `Slf4jHttpLookupPostRequestCallback`.                                                                                                                                                                                        |
| gid.connector.http.source.lookup.connection.timeout                                  | optional | Source table connection timeout. Default - no value.                                                                                                                                                                                                                                                                                                              |
| gid.connector.http.source.lookup.success-codes                                       | optional | Comma separated http codes considered as success response. Use [1-5]XX for groups and '!' character for excluding.                                                                                                                                                                                                                                                |
| gid.connector.http.source.lookup.retry-codes                                         | optional | Comma separated http codes considered as transient errors. Use [1-5]XX for groups and '!' character for excluding.                                                                                                                                                                                                                                                |
| gid.connector.http.source.lookup.ignored-response-codes                              | optional | Comma separated http codes. Content for these responses will be ignored. Use [1-5]XX for groups and '!' character for excluding. Ignored responses togater with `gid.connector.http.source.lookup.success-codes` are considered as successful.                                                                                                                    |
| gid.connector.http.source.lookup.retry-strategy.type                                 | optional | Auto retry strategy type: fixed-delay (default) or exponential-delay.                                                                                                                                                                                                                                                                                             |
| gid.connector.http.source.lookup.retry-strategy.fixed-delay.delay                    | optional | Fixed-delay interval between retries. Default 1 second. Use with`lookup.max-retries` parameter.                                                                                                                                                                                                                                                                   |
| gid.connector.http.source.lookup.retry-strategy.exponential-delay.initial-backoff    | optional | Exponential-delay initial delay. Default 1 second.                                                                                                                                                                                                                                                                                                                |
| gid.connector.http.source.lookup.retry-strategy.exponential-delay.max-backoff        | optional | Exponential-delay maximum delay. Default 1 minute. Use with `lookup.max-retries` parameter.                                                                                                                                                                                                                                                                       |
| gid.connector.http.source.lookup.retry-strategy.exponential-delay.backoff-multiplier | optional | Exponential-delay multiplier. Default value 1.5                                                                                                                                                                                                                                                                                                                   |

### HTTP Sink

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
| gid.connector.http.sink.request-callback                | optional | Specify which `HttpPostRequestCallback` implementation to use. By default, it is set to `slf4j-logger` corresponding to `Slf4jHttpPostRequestCallback`.                                                                                          |
| gid.connector.http.sink.error.code                      | optional | List of HTTP status codes that should be treated as errors by HTTP Sink, separated with comma.                                                                                                                                                   |
| gid.connector.http.sink.error.code.exclude              | optional | List of HTTP status codes that should be excluded from the `gid.connector.http.sink.error.code` list, separated with comma.                                                                                                                      |
| gid.connector.http.security.cert.server                 | optional | Path to trusted HTTP server certificate that should be add to connectors key store. More than one path can be specified using `,` as path delimiter.                                                                                             |
| gid.connector.http.security.cert.client                 | optional | Path to trusted certificate that should be used by connector's HTTP client for mTLS communication.                                                                                                                                               |
| gid.connector.http.security.key.client                  | optional | Path to trusted private key that should be used by connector's HTTP client for mTLS communication.                                                                                                                                               |
| gid.connector.http.security.cert.server.allowSelfSigned | optional | Accept untrusted certificates for TLS communication.                                                                                                                                                                                             |
| gid.connector.http.sink.request.timeout                 | optional | Sets HTTP request timeout in seconds. If not specified, the default value of 30 seconds will be used.                                                                                                                                            |
| gid.connector.http.sink.writer.thread-pool.size         | optional | Sets the size of pool thread for HTTP Sink request processing. Increasing this value would mean that more concurrent requests can be processed in the same time. If not specified, the default value of 1 thread will be used.                   |
| gid.connector.http.sink.writer.request.mode             | optional | Sets Http Sink request submission mode. Two modes are available to select, `single` and `batch` which is the default mode if option is not specified.                                                                                            |
| gid.connector.http.sink.request.batch.size              | optional | Applicable only for `gid.connector.http.sink.writer.request.mode = batch`. Sets number of individual events/requests that will be submitted as one HTTP request by HTTP sink. The default value is 500 which is same as HTTP Sink `maxBatchSize` |


## Lookup Cache 
The HTTP Client connector can be used in lookup join as a lookup source (also known as a dimension table).  

By default, the lookup cache is not enabled. You can enable it by setting `lookup.cache` to `PARTIAL`.
The scope of the cache is per job, so long-running jobs can benefit from this caching.

The lookup cache is used to improve the performance of temporal joins. By default, the lookup cache is not enabled,
so all the API requests are sent on the network. When the lookup cache is enabled, Flink looks in the cache first,
and only sends requests on the network when there is no cached value,  then the cache is updated with the returned rows.
The oldest rows in this cache are expired when the cache hits the max cached rows `lookup.partial-cache.max-rows`
or when the row exceeds the max time to live specified by `lookup.partial-cache.expire-after-write`
or `lookup.partial-cache.expire-after-access`.

By default, flink caches the empty query result for the primary key. You can toggle this behaviour by setting
`lookup.partial-cache.cache-missing-key` to false.


## Build and deployment
To build the project locally you need to have `maven 3` and Java 11+. </br>

Project build command: `mvn package`. </br>
Detailed test report can be found under `target/site/jacoco/index.xml`.

## Demo application
**Note**: This demo works only for Flink-1.15x.

You can test this connector using simple mock http server provided with this repository and Flink SQL-client. 
The mock server can be started from IDE (currently only this way) by running `HttpStubApp::main` method. 
It will start HTTP server listening on `http://localhost:8080/client`

Steps to follow:
- Run Mock HTTP server from `HttpStubApp::main` method.
- Start your Flink cluster, for example as described under https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/try-flink/local_installation/
- Start Flink SQL Client [6] by calling: `./bin/sql-client.sh -j flink-http-connector-1.0-SNAPSHOT.jar`
- Execute SQL statements:
Create Data Stream source Table:
```roomsql
CREATE TABLE Orders (id STRING, id2 STRING, proc_time AS PROCTIME()
) WITH (
'connector' = 'datagen', 
'rows-per-second' = '1', 
'fields.id.kind' = 'sequence', 
'fields.id.start' = '1', 
'fields.id.end' = '120', 
'fields.id2.kind' = 'sequence', 
'fields.id2.start' = '2', 
'fields.id2.end' = '120'
);
```

Create Http Connector Lookup Table:
```roomsql
CREATE TABLE Customers (
	id STRING,
	id2 STRING,
	msg STRING,
	uuid STRING,
	details ROW<
	  isActive BOOLEAN,
	  nestedDetails ROW<
	    balance STRING
	  >
	>
) WITH (
'connector' = 'rest-lookup',
'format' = 'json',
'url' = 'http://localhost:8080/client', 
'asyncPolling' = 'true'
);
```

Submit SQL Select query to join both tables:
```roomsql
SELECT o.id, o.id2, c.msg, c.uuid, c.isActive, c.balance FROM Orders AS o JOIN Customers FOR SYSTEM_TIME AS OF o.proc_time AS c ON o.id = c.id AND o.id2 = c.id2;
```

As a result, you should see a table with joined records like so:
![join-result](docs/JoinTable.PNG)

The `msg` column shows parameters used with REST call for given JOIN record.

## Implementation
### HTTP Source
Implementation of an HTTP source connector is based on Flink's `TableFunction` and `AsyncTableFunction` classes.  
To be more specific we are using a `LookupTableSource`. Unfortunately Flink's new unified source interface [2] cannot be used for this type of source.
Issue was discussed on Flink's user mailing list - https://lists.apache.org/thread/tx2w1m15zt5qnvt924mmbvr7s8rlyjmw

Implementation of an HTTP Sink is based on Flink's `AsyncSinkBase` introduced in Flink 1.15 [3, 4].

#### Http Response to Table schema mapping
The mapping from Http Json Response to SQL table schema is done via Flink's Json Format [5].

## Breaking changes
- Version 0.10
  - Http Sink submission mode changed from single to batch. From now, body of HTTP POUT/POST request will contain a Json array.
  - Changed API for public HttpSink builder. The `setHttpPostRequestCallback` expects a `PostRequestCallback`
    of generic type [HttpRequest](src/main/java/com/getindata/connectors/http/internal/sink/httpclient/HttpRequest.java)
    instead `HttpSinkRequestEntry`.
- Version 0.20
  - Http source table parameters: `gid.connector.http.source.lookup.error.code` and `gid.connector.http.source.lookup.error.code.exclude` were removed. These parameters described http status codes which was silently ignored by source lookup table (logged only). it's not recommended to ignore all error response but it's still possible. To do this set all codes as success: `'gid.connector.http.source.lookup.success-codes' = '2XX'` with ignore body from the others responses than 200s: `'gid.connector.http.source.lookup.ignored-response-codes' = '1XX,3XX,4XX,5XX'`. You can still exclude some error codes marking it as transition errors - `gid.connector.http.source.lookup.retry-codes`. Retry-codes have to be excluded from both `success-codes` and `ignored-response-codes`.
  - Added dependency io.github.resilience4j:resilience4j-retry

## TODO

### HTTP TableLookup Source
- Check other `//TODO`'s.

### HTTP Sink
- Make `HttpSink` retry the failed requests. Currently, it does not retry those at all, only adds their count to the `numRecordsSendErrors` metric. It should be thoroughly thought over how to do it efficiently and then implemented.

### 
[1] https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/dev/table/sql/queries/joins/#lookup-join
</br>
[2] https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/dev/datastream/sources/
</br>
[3] https://cwiki.apache.org/confluence/display/FLINK/FLIP-171%3A+Async+Sink
</br>
[4] https://nightlies.apache.org/flink/flink-docs-release-1.15/api/java/org/apache/flink/connector/base/sink/AsyncSinkBase.html
</br>
[5] https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/table/formats/json/
</br>
[6] https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/sqlclient/
</br>
