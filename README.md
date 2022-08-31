# flink-http-connector

[![Maven Central](https://img.shields.io/maven-central/v/com.getindata/flink-http-connector)](https://mvnrepository.com/artifact/com.getindata/flink-http-connector)
[![javadoc](https://javadoc.io/badge2/com.getindata/flink-http-connector/javadoc.svg)](https://javadoc.io/doc/com.getindata/flink-http-connector) 

The HTTP TableLookup connector that allows for pulling data from external system via HTTP GET method and HTTP Sink that allows for sending data to external system via HTTP requests.

**Note**: The `main` branch may be in an *unstable or even broken state* during development.
Please use [releases](https://github.com/getindata/flink-http-connector/releases) instead of the `main` branch in order to get a stable set of binaries.

The goal for HTTP TableLookup connector was to use it in Flink SQL statement as a standard table that can be later joined with other stream using pure SQL Flink.
 
Currently, HTTP source connector supports only Lookup Joins (TableLookup) [1] in Table/SQL API.
`HttpSink` supports both Streaming API (when using [HttpSink](src/main/java/com/getindata/connectors/http/internal/sink/HttpSink.java) built using [HttpSinkBuilder](src/main/java/com/getindata/connectors/http/internal/sink/HttpSinkBuilder.java)) and the Table API (using connector created in [HttpDynamicTableSinkFactory](src/main/java/com/getindata/connectors/http/internal/table/HttpDynamicTableSinkFactory.java)). 

## Prerequisites
* Java 11
* Maven 3
* Flink 1.15+

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

The columns and their values used for JOIN `ON` condition will be used as HTTP get parameters where the column name will be used as a request parameter name. 
For Example: 
``
http://localhost:8080/client/service?id=1&uuid=2
``
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

#### Custom request/response callback
Http Sink processes responses that it gets from the HTTP endpoint along their respective requests. One can customize the
behaviour of the additional stage of processing done by Table API Sink by implementing
[HttpPostRequestCallback](src/main/java/com/getindata/connectors/http/HttpPostRequestCallback.java) and
[HttpPostRequestCallbackFactory](src/main/java/com/getindata/connectors/http/HttpPostRequestCallbackFactory.java)
interfaces. Custom implementations of `HttpSinkRequestCallbackFactory` can be registered along other factories in
`resources/META-INF.services/org.apache.flink.table.factories.Factory` file and then referenced by their identifiers in
the HttpSink DDL property field `gid.connector.http.sink.request-callback`.

A default implementation that logs those pairs as *INFO* level logs using Slf4j
([Slf4jHttpPostRequestCallback](src/main/java/com/getindata/connectors/http/internal/table/sink/Slf4jHttpPostRequestCallback.java))
is provided.

## HTTP status code handler
Http Sink and Lookup Source connectors allow defining list of HTTP status codes that should be treated as errors. 
By default all 400s and 500s response codes will be interpreted as error code.

This behavior can be changed by using below properties in table definition (DDL) for Sink and Lookup Source or passing it via 
`setProperty' method from Sink's builder. The property names are:
- `gid.connector.http.sink.error.code` and `gid.connector.http.source.lookup.error.code` used to defined HTTP status code value that should be treated as error for example 404.
Many status codes can be defined in one value, where each code should be separated with comma, for example:
`401, 402, 403`. User can use this property also to define a type code mask. In that case, all codes from given HTTP response type will be treated as errors.
An example of such a mask would be `3XX, 4XX, 5XX`. In this case, all 300s, 400s and 500s status codes will be treated as errors.
- `gid.connector.http.sink.error.code.exclude` and `gid.connector.http.source.lookup.error.code.exclude` used to exclude a HTTP code from error list.
   Many status codes can be defined in one value, where each code should be separated with comma, for example:
  `401, 402, 403`. In this example, codes 401, 402 and 403 would not be interpreted as error codes.


## Table API Connector Options
### HTTP TableLookup Source
| Option                                       | Required | Description/Value                                                                                                             |
|----------------------------------------------|----------|-------------------------------------------------------------------------------------------------------------------------------|
| connector                                    | required | The Value should be set to _rest-lookup_                                                                                      |
| format                                       | required | Flink's format name that should be used to decode REST response, Use `json` for a typical REST endpoint.                      |
| url                                          | required | The base URL that should be use for GET requests. For example _http://localhost:8080/client_                                  |
| asyncPolling                                 | optional | true/false - determines whether Async Pooling should be used. Mechanism is based on Flink's Async I/O.                        |
| gid.connector.http.lookup.error.code         | optional | List of HTTP status codes that should be treated as errors by HTTP Source, separated with comma.                              |
| gid.connector.http.lookup.error.code.exclude | optional | List of HTTP status codes that should be excluded from the `gid.connector.http.lookup.error.code` list, separated with comma. |

### HTTP Sink
| Option                                     | Required | Description/Value                                                                                                                                                                                  |
|--------------------------------------------|----------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| connector                                  | required | Specify what connector to use. For HTTP Sink it should be set to _'http-sink'_.                                                                                                                    |
| url                                        | required | The base URL that should be use for HTTP requests. For example _http://localhost:8080/client_.                                                                                                     |
| format                                     | required | Specify what format to use.                                                                                                                                                                        |
| insert-method                              | optional | Specify which HTTP method to use in the request. The value should be set either to `POST` or `PUT`.                                                                                                |
| sink.batch.max-size                        | optional | Maximum number of elements that may be passed in a batch to be written downstream.                                                                                                                 |
| sink.requests.max-inflight                 | optional | The maximum number of in flight requests that may exist, if any more in flight requests need to be initiated once the maximum has been reached, then it will be blocked until some have completed. |
| sink.requests.max-buffered                 | optional | Maximum number of buffered records before applying backpressure.                                                                                                                                   |
| sink.flush-buffer.size                     | optional | The maximum size of a batch of entries that may be sent to the HTTP endpoint measured in bytes.                                                                                                    |
| sink.flush-buffer.timeout                  | optional | Threshold time in milliseconds for an element to be in a buffer before being flushed.                                                                                                              |
| gid.connector.http.sink.request-callback   | optional | Specify which `HttpPostRequestCallback` implementation to use. By default, it is set to `slf4j-logger` corresponding to `Slf4jHttpPostRequestCallback`.                                            |
| gid.connector.http.sink.error.code         | optional | List of HTTP status codes that should be treated as errors by HTTP Sink, separated with comma.                                                                                                     |
| gid.connector.http.sink.error.code.exclude | optional | List of HTTP status codes that should be excluded from the `gid.connector.http.sink.error.code` list, separated with comma.                                                                        |

## Build and deployment
To build the project locally you need to have `maven 3` and Java 11+. </br>

Project build command: `mvn package`. </br>
Detailed test report can be found under `target/site/jacoco/index.xml`.

## Demo application
You can test this connector using simple mock http server provided with this repository and Flink SQL-client. 
The mock server can be started from IDE (currently only this way) by running `HttpStubApp::main` method. 
It will start HTTP server listening on `http://localhost:8080/client`

Steps to follow:
- Run Mock HTTP server from `HttpStubApp::main` method.
- Start your Flink cluster, for example as described under https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/try-flink/local_installation/
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
'format' = 'json' 
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

## TODO

### HTTP TableLookup Source
- Implement caches.
- Think about Retry Policy for Http Request
- Add Configurable Timeout value
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
