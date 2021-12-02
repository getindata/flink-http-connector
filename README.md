# flink-http-connector
The HTTP TableLookup connector that allows for pulling data from external system via HTTP GET method.
The goal for this connector was to use it in Flink SQL statement as a standard table that can be later joined with other stream using pure SQL Flink.
 
Currently, connector supports only Lookup Joins [1] and expects JSON as a response body.

Connector supports only STRING types.

## Prerequisites
* Java 11
* Maven 3
* Flink 14+

## Structure and further work
The main code can be found under `com.getindata.connectors.http.table` package plus additional classes directly from `com.getindata.connectors.http`

The `com.getindata.connectors.http.stream` package is pure PoC and currently not meant to be use. The purpose of this package was to test out the new Unified Source design for Flink Source Connector
[2]. Currently, the implementation is purely PoC, and requires further development.

## Usage
Flink SQL table definition:

```roomsql
CREATE TABLE Customers (
id STRING,
id2 STRING,
msg STRING,
uuid STRING,
isActive STRING,
balance STRING
) WITH (
'connector' = 'rest-lookup',
'url' = 'http://localhost:8080/client',
'asyncPolling' = 'true',
'field.isActive.path' = '$.details.isActive',
'field.balance.path' = '$.details.nestedDetails.balance'
)
```
Using _Customers_ table in Flink SQL Lookup Join:

```roomsql
SELECT o.id, o.id2, c.msg, c.uuid, c.isActive, c.balance FROM Orders AS o 
JOIN Customers FOR SYSTEM_TIME AS OF o.proc_time AS c ON o.id = c.id AND o.id2 = c.id2
```

The columns and their values used for JOIN `ON` condition will be used as HTTP get parameters where the column name will be used as a request parameter name. 
For Example: 
``
http://localhost:8080/client/service?id=1&uuid=2
``

## Http Response to Table schema mapping
The mapping from Http Json Response to SQL table schema is done via Json Paths [3]. 
This is achieved thanks to `com.jayway.jsonpath:json-path` library.

If no `root` or `field.#.path` option is defined, the connector will use the column name as json path and will try to look for Json Node with that name in received Json. If no node with a given name is found, the connector will return `null` as value for this field. 

If the `field.#.path` option is defined, connector will use given Json path from option's value in order to find Json data that should be used for this column. 
For example `'field.isActive.path' = '$.details.isActive'` - the value for table column `isActive` will be taken from `$.details.isActive` node from received Json.

## Connector Options
| Option | Required | Description/Value|
| -------------- | ----------- | -------------- |
| connector |  required | The Value should be set to _rest-lookup_|
| url | required | The base URL that should be use for GET requests. For example _http://localhost:8080/client_|
| asyncPolling | optional | true/false - determines whether Async Pooling should be used. Mechanism is based on Flink's Async I/O.|
| root | optional | Sets the json root node for entire table. The value should be presented as Json Path [3], for example `$.details`.|
| field.#.path | optional | The Json Path from response model that should be use for given `#` field. If `root` option was defined it will be added to field path. The value must be presented in Json Path format [3], for example `$.details.nestedDetails.balance` |

## TODO
- Implement caches.
- Add support for other Flink types. Currently, STRING type is only fully supported.
- Think about Retry Policy for Http Request
- Use Flink Format [4] to parse Json response 
- Add Configurable Timeout value
- Check other `//TODO`'s.

### 
[1] https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/table/sql/queries/joins/#lookup-join
</br>
[2] https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/datastream/sources/
</br>
[3] https://support.smartbear.com/alertsite/docs/monitors/api/endpoint/jsonpath.html
</br>
[4] https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/table/formats/json/