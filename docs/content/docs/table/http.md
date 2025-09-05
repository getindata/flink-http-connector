
---
title: HTTP
weight: 3
type: docs
aliases:
- /dev/table/connectors/http.html
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

# HTTP Connector

{{< label "Sink: Streaming Append Mode" >}}
{{< label "Lookup Source: Sync Mode" >}}
{{< label "Lookup Source: Async Mode" >}}
{{< label "Sink: Batch" >}}


The HTTP connector allows for pulling data from external system via HTTP methods and HTTP Sink that allows for sending data to external system via HTTP requests.

The HTTP source connector supports [Lookup Joins](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/sourcessinks/#lookup-table-source) in [Table API and SQL](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/overview/).

<!-- TOC -->
* [HTTP Connector](#http-connector)
  * [Dependencies](#dependencies)
  * [Working with HTTP lookup source tables](#working-with-http-lookup-source-tables)
    * [HTTP Lookup Table API and SQL Source example](#http-lookup-table-api-and-sql-source-example)
    * [Using a HTTP Lookup Source in a lookup join](#using-a-http-lookup-source-in-a-lookup-join)
    * [Lookup Source Connector Options](#lookup-source-connector-options)
    * [Query Creators](#query-creators)
    * [generic-json-url Query Creator](#generic-json-url-query-creator)
    * [generic-json-url Query Creator](#generic-json-url-query-creator-1)
    * [Http headers](#http-headers)
    * [Timeouts](#timeouts)
    * [Source table HTTP status code](#source-table-http-status-code)
    * [Retries (Lookup source)](#retries-lookup-source)
        * [Retry strategy](#retry-strategy)
      * [Lookup multiple results](#lookup-multiple-results)
  * [Working with HTTP sink tables](#working-with-http-sink-tables)
    * [HTTP Sink](#http-sink)
    * [Sink Connector Options](#sink-connector-options)
    * [Sink table HTTP status codes](#sink-table-http-status-codes)
    * [Request submission](#request-submission)
    * [Batch submission mode](#batch-submission-mode)
    * [Single submission mode](#single-submission-mode)
  * [Available Metadata](#available-metadata)
  * [HTTP status code handler](#http-status-code-handler)
  * [Security considerations](#security-considerations)
    * [TLS (more secure replacement for SSL) and mTLS support](#tls-more-secure-replacement-for-ssl-and-mtls-support)
    * [Basic Authentication](#basic-authentication)
    * [OIDC Bearer Authentication](#oidc-bearer-authentication)
      * [Restrictions at this time](#restrictions-at-this-time)
<!-- TOC -->
## Dependencies

{{< sql_connector_download_table "http" >}}

The HTTP connector is not part of the binary distribution.
See how to link with it for cluster execution [here]({{< ref "docs/dev/configuration/overview" >}}).

## Migration from GetInData HTTP connector

The GetInData HTTP connector was donated to Flink in [FLIP-532](https://cwiki.apache.org/confluence/display/FLINK/FLIP-532%3A+Donate+GetInData+HTTP+Connector+to+Flink). The Flink connector has the same capabilities as the original connector.
The Flink connector does have some changes that you need to be aware of if you are migrating from using the original connector:

* Existing java applications will need to be recompiled to pick up the new flink package names.
* Existing application and SQL need to be amended to use the new connector option names. The new option names do not have 
the _com.getindata.http_ prefix, the prefix is now _http_ prefix.

## Working with HTTP lookup source tables

### HTTP Lookup Table API and SQL Source example
Here is an example Flink SQL Enrichment Lookup Table definition:

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

### Using a HTTP Lookup Source in a lookup join

To easy see how the lookup enrichment works, we can define a data source using datagen:
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

Then we can enrich the _Orders_ table with the _Customers_ HTTP table with the following SQL:

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

### Lookup Source Connector Options

Note the options with the prefix _http_ are the HTTP connector specific options, the others are Flink options.   

| Option                                                                 | Required | Description/Value                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
|:-----------------------------------------------------------------------|----------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| connector                                                              | required | The Value should be set to _rest-lookup_                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| format                                                                 | required | Flink's format name that should be used to decode REST response, Use `json` for a typical REST endpoint.                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| url                                                                    | required | The base URL that should be use for GET requests. For example _http://localhost:8080/client_                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| asyncPolling                                                           | optional | true/false - determines whether Async Polling should be used. Mechanism is based on Flink's Async I/O.                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| lookup-method                                                          | optional | GET/POST/PUT (and any other) - determines what REST method should be used for lookup REST query. If not specified, `GET` method will be used.                                                                                                                                                                                                                                                                                                                                                                                                                    |
| lookup.cache                                                           | optional | Enum possible values: `NONE`, `PARTIAL`. The cache strategy for the lookup table. Currently supports `NONE` (no caching) and `PARTIAL` (caching entries on lookup operation in external API).                                                                                                                                                                                                                                                                                                                                                                    |
| lookup.partial-cache.max-rows                                          | optional | The max number of rows of lookup cache, over this value, the oldest rows will be expired. `lookup.cache` must be set to `PARTIAL` to use this option. See the following <a href="#lookup-cache">Lookup Cache</a> section for more details.                                                                                                                                                                                                                                                                                                                       |
| lookup.partial-cache.expire-after-write                                | optional | The max time to live for each rows in lookup cache after writing into the cache. Specify as a [Duration](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/deployment/config/#duration).  `lookup.cache` must be set to `PARTIAL` to use this option. See the following <a href="#lookup-cache">Lookup Cache</a> section for more details.                                                                                                                                                                                                         |
| lookup.partial-cache.expire-after-access                               | optional | The max time to live for each rows in lookup cache after accessing the entry in the cache. Specify as a [Duration](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/deployment/config/#duration). `lookup.cache` must be set to `PARTIAL` to use this option. See the following <a href="#lookup-cache">Lookup Cache</a> section for more details.                                                                                                                                                                                                |
| lookup.partial-cache.cache-missing-key                                 | optional | This is a boolean that defaults to true. Whether to store an empty value into the cache if the lookup key doesn't match any rows in the table. `lookup.cache` must be set to `PARTIAL` to use this option. See the following <a href="#lookup-cache">Lookup Cache</a> section for more details.                                                                                                                                                                                                                                                                  |
| lookup.max-retries                                                     | optional | The max retry times if the lookup failed; default is 3. See the following <a href="#lookup-cache">Lookup Cache</a> section for more detail. Set value 0 to disable retries.                                                                                                                                                                                                                                                                                                                                                                                      |
| lookup.error.code                                                      | optional | List of HTTP status codes that should be treated as errors by HTTP Source, separated with comma.                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| lookup.error.code.exclude                                              | optional | List of HTTP status codes that should be excluded from the `http.lookup.error.code` list, separated with comma.                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| http.security.cert.server                                              | optional | Comma separated paths to trusted HTTP server certificates that should be added to the connectors trust store.                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| http.security.cert.client                                              | optional | Path to trusted certificate that should be used by connector's HTTP client for mTLS communication.                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| http.security.key.client                                               | optional | Path to trusted private key that should be used by connector's HTTP client for mTLS communication.                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| http.security.cert.server.allowSelfSigned                              | optional | Accept untrusted certificates for TLS communication.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| http.security.oidc.token.request                                       | optional | OIDC `Token Request` body in `application/x-www-form-urlencoded` encoding                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| http.security.oidc.token.endpoint.url                                  | optional | OIDC `Token Endpoint` url, to which the token request will be issued                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| http.security.oidc.token.expiry.reduction                              | optional | OIDC tokens will be requested if the current time is later than the cached token expiry time minus this value.                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| http.source.lookup.request.timeout                                     | optional | Sets HTTP request timeout in seconds. If not specified, the default value of 30 seconds will be used.                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| http.source.lookup.request.thread-pool.size                            | optional | Sets the size of pool thread for HTTP lookup request processing. Increasing this value would mean that more concurrent requests can be processed in the same time. If not specified, the default value of 8 threads will be used.                                                                                                                                                                                                                                                                                                                                |
| http.source.lookup.response.thread-pool.size                           | optional | Sets the size of pool thread for HTTP lookup response processing. Increasing this value would mean that more concurrent requests can be processed in the same time. If not specified, the default value of 4 threads will be used.                                                                                                                                                                                                                                                                                                                               |
| http.source.lookup.use-raw-authorization-header                        | optional | If set to `'true'`, uses the raw value set for the `Authorization` header, without transformation for Basic Authentication (base64, addition of "Basic " prefix). If not specified, defaults to `'false'`.                                                                                                                                                                                                                                                                                                                                                       |
| http.source.lookup.request-callback                                    | optional | Specify which `HttpLookupPostRequestCallback` implementation to use. By default, it is set to `slf4j-lookup-logger` corresponding to `Slf4jHttpLookupPostRequestCallback`.                                                                                                                                                                                                                                                                                                                                                                                       |
| http.source.lookup.connection.timeout                                  | optional | Source table connection timeout. Default - no value.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| http.source.lookup.success-codes                                       | optional | Comma separated http codes considered as success response. Use [1-5]XX for groups and '!' character for excluding.                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| http.source.lookup.retry-codes                                         | optional | Comma separated http codes considered as transient errors. Use [1-5]XX for groups and '!' character for excluding.                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| http.source.lookup.ignored-response-codes                              | optional | Comma separated http codes. Content for these responses will be ignored. Use [1-5]XX for groups and '!' character for excluding. Ignored responses togater with `http.source.lookup.success-codes` are considered as successful.                                                                                                                                                                                                                                                                                                                                 |
| http.source.lookup.retry-strategy.type                                 | optional | Auto retry strategy type: fixed-delay (default) or exponential-delay.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| http.source.lookup.retry-strategy.fixed-delay.delay                    | optional | Fixed-delay interval between retries. Default 1 second. Use with`lookup.max-retries` parameter.                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| http.source.lookup.retry-strategy.exponential-delay.initial-backoff    | optional | Exponential-delay initial delay. Default 1 second.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| http.source.lookup.retry-strategy.exponential-delay.max-backoff        | optional | Exponential-delay maximum delay. Default 1 minute. Use with `lookup.max-retries` parameter.                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| http.source.lookup.retry-strategy.exponential-delay.backoff-multiplier | optional | Exponential-delay multiplier. Default value 1.5                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| http.source.lookup.proxy.host                                          | optional | Specify the hostname of the proxy.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| http.source.lookup.proxy.port                                          | optional | Specify the port of the proxy.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| http.source.lookup.proxy.username                                      | optional | Specify the username used for proxy authentication.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| http.source.lookup.proxy.password                                      | optional | Specify the password used for proxy authentication.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| http.request.query-param-fields                                        | optional | Used for the `GenericJsonAndUrlQueryCreator` query creator. The names of the fields that will be mapped to query parameters. The parameters are separated by semicolons, such as `param1;param2`.                                                                                                                                                                                                                                                                                                                                                                |                                                                                                                                                                                                                                                                                                                                                               
| http.request.body-fields                                               | optional | Used for the `GenericJsonAndUrlQueryCreator` query creator. The names of the fields that will be mapped to the body. The parameters are separated by semicolons, such as `param1;param2`.                                                                                                                                                                                                                                                                                                                                                                        |                                                                                                                                                                                                                                                                                                                                                                         |
| http.request.url-map                                                   | optional | Used for the `GenericJsonAndUrlQueryCreator` query creator. The map of insert names to column names used as url segments. Parses a string as a map of strings. For example if there are table columns called `customerId` and `orderId`, then specifying value `customerId:cid1,orderID:oid` and a url of https://myendpoint/customers/{cid}/orders/{oid} will mean that the url used for the lookup query will dynamically pickup the values for `customerId`, `orderId` and use them in the url. The expected format of the map is: `key1:value1,key2:value2`. |

### Query Creators

In the above example we see that HTTP GET operations and HTTP POST operations result in different mapping of the columns to the 
HTTP request content. In reality, you will want to have move control over how the SQL columns are mapped to the HTTP content.
The HTTP connector supplies a number of Query Creators that you can use define these mappings.

<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 76%">Name</th>
      <th class="text-center" style="width: 8%">Query param mapping</th>
  <th class="text-center" style="width: 8%">URL path mapping</th>
  <th class="text-center" style="width: 8%">Body mapping</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>generic-json-url</h5></td>
      <td>✓</td>
      <td>✓</td>
      <td>✓</td>
    </tr>
    <tr>
      <td><h5>generic-get-query</h5></td>
      <td><✓ for GETs/td>
      <td></td>
      <td>✓ for PUTs and POSTs</td>
    </tr>
  
    </tbody>
</table>

### generic-json-url Query Creator

The recommended Query creator for json is called _generic-json-url_, which allows column content to be mapped as URL, path, body and query parameter request values; it supports
POST, PUT and GET operations. This query creator allows you to issue json requests without needing to code
your own custom http connector. The mappings from columns to the json request are supplied in the query creator configuration
parameters `http.request.query-param-fields`, `http.request.body-fields` and `http.request.url-map`.

### generic-json-url Query Creator

The default Query Creator is called _generic-json-url_.  For body based queries such as POST/PUT requests, the
([GenericGetQueryCreator](flink-connector-http/src/main/java/org/apache/flink/connectors/http/internal/table/lookup/querycreators/GenericGetQueryCreator.java))is provided as a default query creator. This implementation uses Flink's [json-format](https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/table/formats/json/)  to convert RowData object into Json String.
For GET requests can be used for query parameter based queries. 

The _generic-json-url_ allows for using custom formats that will perform serialization to Json. Thanks to this, users can create their own logic for converting RowData to Json Strings suitable for their HTTP endpoints and use this logic as custom format
with HTTP Lookup connector and SQL queries.
To create a custom format user has to implement Flink's `SerializationSchema` and `SerializationFormatFactory` interfaces and register custom format factory along other factories in
`resources/META-INF.services/org.apache.flink.table.factories.Factory` file. This is common Flink mechanism for providing custom implementations for various factories.

### Http headers
It is possible to set HTTP headers that will be added to HTTP request send by lookup source connector.
Headers are defined via property key `http.source.lookup.header.HEADER_NAME = header value` for example:
`http.source.lookup.header.X-Content-Type-Options = nosniff`.

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
  'http.source.lookup.header.Origin' = '*',
  'http.source.lookup.header.X-Content-Type-Options' = 'nosniff',
  'http.source.lookup.header.Content-Type' = 'application/json'
)
```

Note that when using OIDC, it adds an `Authentication` header with the bearer token; this will override
an existing `Authorization` header specified in configuration.

### Timeouts
Lookup Source is guarded by two timeout timers. First one is specified by Flink's AsyncIO operator that executes `AsyncTableFunction`.
The default value of this timer is set to 3 minutes and can be changed via `table.exec.async-lookup.timeout` [option](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/config/#table-exec-async-lookup-timeout).

The second one is set per individual HTTP requests by HTTP client. Its default value is set currently to 30 seconds and can be changed via `http.source.lookup.request.timeout` option.

Flink's current implementation of `AsyncTableFunction` does not allow specifying custom logic for handling Flink AsyncIO timeouts as it is for Java API.
Because of that, if AsyncIO timer passes, Flink will throw TimeoutException which will cause job restart.

### Source table HTTP status code
The source table categorizes HTTP responses into three groups based on status codes:
- Retry codes (`http.source.lookup.retry-codes`):
  Responses in this group indicate a temporary issue (it can be e.g., HTTP 503 Service Unavailable). When such a response is received, the request should be retried.
- Success codes (`http.source.lookup.success-codes`):
  These are expected responses that should be processed by table function.
- Ignored responses (`http.source.lookup.ignored-response-codes`):
  Successful response, but its content will be ignored. For example, an HTTP 404 Not Found response is valid and indicates that the requested item does not exist, so its content can be ignored.
- Error codes:
  Any response code that is not classified as a retry or success code falls into this category. Receiving such a response will result in a job failure.


### Retries (Lookup source)
Lookup source handles auto-retries for two scenarios:
1. IOException occurs (e.g. temporary network outage)
2. The response contains a HTTP error code that indicates a retriable error. These codes are defined in the table configuration (see `http.source.lookup.retry-codes`).
   Retries are executed silently, without restarting the job. After reaching max retries attempts (per request) operation will fail and restart job.

Notice that HTTP codes are categorized into into 3 groups:
- successful responses - response is returned immediately for further processing
- temporary errors - request will be retried up to the retry limit
- error responses - unexpected responses are not retried and will fail the job. Any HTTP error code which is not configured as successful or temporary error is treated as an unretriable error.

##### Retry strategy
User can choose retry strategy type for source table:
- fixed-delay - http request will be re-sent after specified delay.
- exponential-delay - request will be re-sent with exponential backoff strategy, limited by `lookup.max-retries` attempts. The delay for each retry is calculated as the previous attempt's delay multiplied by the backoff multiplier (parameter `http.source.lookup.retry-strategy.exponential-delay.backoff-multiplier`) up to `http.source.lookup.retry-strategy.exponential-delay.max-backoff`. The initial delay value is defined in the table configuration as `http.source.lookup.retry-strategy.exponential-delay.initial-backoff`.


#### Lookup multiple results

Typically, join can return zero, one or more results. What is more, there are lots of possible REST API designs and
pagination methods. Currently, the connector supports only two simple approaches (`http.source.lookup.result-type`):

- `single-value` - REST API returns single object.
- `array` - REST API returns array of objects. Pagination is not supported yet.

## Working with HTTP sink tables

### HTTP Sink
The following example shows the minimum Table API example to create a sink: 

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

When `'format' = 'json'` is specified on the table definition, the HTTP sink sends json payloads. It is possible to change the format of the payload by specifying
another format name. 

### Sink Connector Options

| Option                                    | Required | Description/Value                                                                                                                                                                                                                  |
|-------------------------------------------|----------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| connector                                 | required | Specify what connector to use. For HTTP Sink it should be set to _'http-sink'_.                                                                                                                                                    |
| format                                    | required | Specify what format to use.                                                                                                                                                                                                        |
| url                                       | required | The base URL that should be use for HTTP requests. For example _http://localhost:8080/client_.                                                                                                                                     |
| insert-method                             | optional | Specify which HTTP method to use in the request. The value should be set either to `POST` or `PUT`.                                                                                                                                |
| sink.batch.max-size                       | optional | Maximum number of elements that may be passed in a batch to be written downstream.                                                                                                                                                 |
| sink.requests.max-inflight                | optional | The maximum number of in flight requests that may exist, if any more in flight requests need to be initiated once the maximum has been reached, then it will be blocked until some have completed.                                 |
| sink.requests.max-buffered                | optional | Maximum number of buffered records before applying backpressure.                                                                                                                                                                   |
| sink.flush-buffer.size                    | optional | The maximum size of a batch of entries that may be sent to the HTTP endpoint measured in bytes.                                                                                                                                    |
| sink.flush-buffer.timeout                 | optional | Threshold time in milliseconds for an element to be in a buffer before being flushed.                                                                                                                                              |
| http.sink.request-callback                | optional | Specify which `HttpPostRequestCallback` implementation to use. By default, it is set to `slf4j-logger` corresponding to `Slf4jHttpPostRequestCallback`.                                                                            |
| http.sink.error.code                      | optional | List of HTTP status codes that should be treated as errors by HTTP Sink, separated with comma.                                                                                                                                     |
| http.sink.error.code.exclude              | optional | List of HTTP status codes that should be excluded from the `http.sink.error.code` list, separated with comma.                                                                                                                      |
| http.security.cert.server                 | optional | Path to trusted HTTP server certificate that should be add to connectors key store. More than one path can be specified using `,` as path delimiter.                                                                               |
| http.security.cert.client                 | optional | Path to trusted certificate that should be used by connector's HTTP client for mTLS communication.                                                                                                                                 |
| http.security.key.client                  | optional | Path to trusted private key that should be used by connector's HTTP client for mTLS communication.                                                                                                                                 |
| http.security.cert.server.allowSelfSigned | optional | Accept untrusted certificates for TLS communication.                                                                                                                                                                               |
| http.sink.request.timeout                 | optional | Sets HTTP request timeout in seconds. If not specified, the default value of 30 seconds will be used.                                                                                                                              |
| http.sink.writer.thread-pool.size         | optional | Sets the size of pool thread for HTTP Sink request processing. Increasing this value would mean that more concurrent requests can be processed in the same time. If not specified, the default value of 1 thread will be used.     |
| http.sink.writer.request.mode             | optional | Sets Http Sink request submission mode. Two modes are available to select, `single` and `batch` which is the default mode if option is not specified.                                                                              |
| http.sink.request.batch.size              | optional | Applicable only for `http.sink.writer.request.mode = batch`. Sets number of individual events/requests that will be submitted as one HTTP request by HTTP sink. The default value is 500 which is same as HTTP Sink `maxBatchSize` |

### Sink table HTTP status codes
You can configure a list of HTTP status codes that should be treated as errors for HTTP sink table.
By default all 400 and 500 response codes will be interpreted as error code.

This behavior can be changed by using below properties in table definition. The property name are:
- `http.sink.error.code` used to defined HTTP status code value that should be treated as error for example 404.
  Many status codes can be defined in one value, where each code should be separated with comma, for example:
  `401, 402, 403`. User can use this property also to define a type code mask. In that case, all codes from given HTTP response type will be treated as errors.
  An example of such a mask would be `3XX, 4XX, 5XX`. In this case, all 300s, 400s and 500s status codes will be treated as errors.
- `http.sink.error.code.exclude` used to exclude a HTTP code from error list.
  Many status codes can be defined in one value, where each code should be separated with comma, for example:
  `401, 402, 403`. In this example, codes 401, 402 and 403 would not be interpreted as error codes.


### Request submission
HTTP Sink by default submits events in batch. The submission mode can be changed using `http.sink.writer.request.mode` property using `single` or `batch` as property value.

### Batch submission mode
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

```roomsql
CREATE TABLE http (
  id bigint,
  some_field string
) WITH (
  'connector' = 'http-sink',
  'url' = 'http://example.com/myendpoint',
  'format' = 'json',
  'http.sink.request.batch.size' = '50'
)
```

### Single submission mode
In this mode every processed event is submitted as individual HTTP POST/PUT request.

SQL:
```roomsql
CREATE TABLE http (
  id bigint,
  some_field string
) WITH (
  'connector' = 'http-sink',
  'url' = 'http://example.com/myendpoint',
  'format' = 'json',
  'http.sink.writer.request.mode' = 'single'
)
```

## Available Metadata

The is no available metadata for this connector.

## HTTP status code handler

Above parameters support include lists and exclude lists. A sample configuration may look like this:
`2XX,404,!203` - meaning all codes from group 2XX (200-299), with 404 and without 203 ('!' character). Group exclude listing e.g. !2XX is not supported.

The same format is used in parameter `http.source.lookup.retry-codes`.

Example with explanation:
```roomsql
CREATE TABLE [...]
WITH (
  [...],
  'http.source.lookup.success-codes' = '2XX',
  'http.source.lookup.retry-codes' = '5XX,!501,!505,!506',
  'http.source.lookup.ignored-response-codes' = '404'
)
```
All 200s codes and 404 are considered as successful (`success-codes`, `ignored-response-codes`). These responses won't cause retry or job failure. 404 response is listed in `ignored-response-codes` parameter, what means content body will be ignored. Http with 404 code will produce just empty record.
When server returns response with 500s code except 501, 505 and 506 then connector will re-send request based on configuration in `http.source.lookup.retry-strategy` parameters. By default it's fixed-delay with 1 second delay, up to 3 times per request (parameter `lookup.max-retries`). After exceeding max-retries limit the job will fail.
A response with any other code than specified in params `success-codes` and `retry-codes` e.g. 400, 505, 301 will cause job failure.


```roomsql
CREATE TABLE [...]
WITH (
  [...],
  'http.source.lookup.success-codes' = '2XX',
  'http.source.lookup.retry-codes' = '',
  'http.source.lookup.ignored-response-codes' = '1XX,3XX,4XX,5XX'
)
```
In this configuration, all HTTP responses are considered successful because the sets `success-codes` and `ignored-response-codes` together cover all possible status codes. As a result, no retries will be triggered based on HTTP response codes. However, only responses with status code 200 will be parsed and processed by the Flink operator. Responses with status codes in the 1xx, 3xx, 4xx, and 5xx ranges are classified under `ignored-response-codes`.
Note that retries remain enabled and will still occur on IOException.
To disable retries, set `'lookup.max-retries' = '0'`.

## Security considerations

### TLS (more secure replacement for SSL) and mTLS support

Both Http Sink and Lookup Source connectors support HTTPS communication using TLS 1.2 and mTLS.
To enable Https communication simply use `https` protocol in endpoint's URL.

To specify certificate(s) to be used by the server, use `http.security.cert.server` connector property;
the value is a comma separated list of paths to certificate(s), for example you can use your organization's CA
Root certificate, or a self-signed certificate.

Note that if there are no security properties for a `https` url then, the JVMs default certificates are
used - allowing use of globally recognized CAs without the need for configuration.

You can also configure the connector to use mTLS. For this simply use `http.security.cert.client`
and `http.security.key.client` connector properties to specify paths to the certificate and
private key. The key MUST be in `PCKS8` format. Both PEM and DER keys are
allowed.

For non production environments it is sometimes necessary to use Https connection and accept all certificates.
In this special case, you can configure connector to trust all certificates without adding them to keystore.
To enable this option use `http.security.cert.server.allowSelfSigned` property setting its value to `true`.

### Basic Authentication
The connector supports Basic Authentication using a HTTP `Authorization` header.
The header value can be set via properties, similarly as for other headers. The connector converts the passed value to Base64 and uses it for the request.
If the used value starts with the prefix `Basic`, or `http.source.lookup.use-raw-authorization-header`
is set to `'true'`, it will be used as header value as is, without any extra modification.

### OIDC Bearer Authentication
The connector supports Bearer Authentication using a HTTP `Authorization` header. The [OAuth 2.0 rcf](https://datatracker.ietf.org/doc/html/rfc6749) mentions [Obtaining Authorization](https://datatracker.ietf.org/doc/html/rfc6749#section-4)
and an authorization grant. OIDC makes use of this [authorisation grant](https://datatracker.ietf.org/doc/html/rfc6749#section-1.3) in a [Token Request](https://openid.net/specs/openid-connect-core-1_0.html#TokenRequest) by including a [OAuth grant type](https://oauth.net/2/grant-types/) and associated properties, the response is the [token response](https://openid.net/specs/openid-connect-core-1_0.html#TokenResponse).

If you want to use this authorization then you should supply the `Token Request` body in `application/x-www-form-urlencoded` encoding
in configuration property `http.security.oidc.token.request`. See [grant extension](https://datatracker.ietf.org/doc/html/rfc6749#section-4.5) for
an example of a customised grant type token request. The supplied `token request` will be issued to the
[token end point](https://datatracker.ietf.org/doc/html/rfc6749#section-3.2), whose url should be supplied in configuration property
`http.security.oidc.token.endpoint.url`. The returned `access token` is then cached and used for subsequent requests; if the token has expired then
a new one is requested. There is a property `http.security.oidc.token.expiry.reduction`, that defaults to 1 second; new tokens will
be requested if the current time is later than the cached token expiry time minus `http.security.oidc.token.expiry.reduction`.

#### Restrictions at this time
* No authentication is applied to the token request.
* The processing does not use the refresh token if it present.
  {{< top >}}
