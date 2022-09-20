# Changelog

## [Unreleased]

### Added

- Add Http Header value preprocessor mechanism, that can preprocess defined header value before setting it on the request.
- Allow user to specify `Authorization` header for Basic Authentication. The value will be converted to Base64,
  or if it starts from prefix `Basic `, it will be used as is (without any extra modification).
- Add TLS and mTLS support for Http Sink and Lookup Source connectors.  
New properties are:
  - `gid.connector.http.security.cert.server` - path to server's certificate.
  - `gid.connector.http.security.cert.client` - path to connector's certificate.
  - `gid.connector.http.security.key.client` - path to connector's private key.
  - `gid.connector.http.security.cert.server.allowSelfSigned` - allowing for self-signed certificates without adding them to KeyStore (not recommended for a production).
- Add [LookupQueryCreator](src/main/java/com/getindata/connectors/http/LookupQueryCreator.java) and
  [LookupQueryCreatorFactory](src/main/java/com/getindata/connectors/http/LookupQueryCreatorFactory.java) interfaces 
  (along with a "default"
  [GenericGetQueryCreator](src/main/java/com/getindata/connectors/http/internal/table/lookup/querycreators/GenericGetQueryCreator.java)
  implementation) for customization of queries prepared by Lookup Source for its HTTP requests.

## [0.4.0] - 2022-08-31

### Added

-   Add new properties `gid.connector.http.sink.error.code`,`gid.connector.http.sink.error.code.exclude`,
    `gid.connector.http.source.lookup.error.code` and `gid.connector.http.source.lookup.error.code.exclude`
    to set HTTP status codes that should be interpreted as errors both for HTTP Sink and HTTP Lookup Source.
-   Use Flink's format support to Http Lookup Source.
-   Add HTTP Lookup source client header configuration via properties.
-   Add [HttpPostRequestCallback](src/main/java/com/getindata/connectors/http/HttpPostRequestCallback.java) and
    [HttpPostRequestCallbackFactory](src/main/java/com/getindata/connectors/http/HttpPostRequestCallbackFactory.java)
    interfaces (along with a "default"
    [Slf4jHttpPostRequestCallback](src/main/java/com/getindata/connectors/http/internal/table/sink/Slf4jHttpPostRequestCallback.java)
    implementation) for customizable processing of HTTP Sink requests and responses in Table API.

### Changed

-   Change dependency scope for `org.apache.flink.flink-connector-base` from `compile` to `provided`.
-   Changed DDL of `rest-lookup` connector. Dropped `json-path` properties, and add mandatory `format` property.

### Removed

-   Remove dependency on `org.apache.httpcomponents.httpclient`from production code. Dependency is only for test scope.
-   Removed dependency on `com.jayway.jsonpath.json-path`

### Fixed

-   Fix JavaDoc errors.

## [0.3.0] - 2022-07-21

-   Package refactoring. Hide internal classes that does not have to be used by API users under "internal" package.
    Methods defined in classes located outside "internal" package are considered "public API".
    Any changes to those methods should be communicated as "not backward compatible" and should be avoided.
-   Add checkstyle configuration to "dev" folder. Add checkstyle check during maven build
-   Add HTTP sink client header configuration via properties.

## [0.2.0] - 2022-07-06

-   Implement [HttpSink](src/main/java/com/getindata/connectors/http/HttpSink.java) deriving from [AsyncSinkBase](https://cwiki.apache.org/confluence/display/FLINK/FLIP-171%3A+Async+Sink) introduced in Flink 1.15.
-   Add support for Table API in HttpSink in the form of [HttpDynamicSink](src/main/java/com/getindata/connectors/http/internal/table/sink/HttpDynamicSink.java). 

## [0.1.0] - 2022-05-26

-   Implement basic support for Http connector for Flink SQL

[Unreleased]: https://github.com/getindata/flink-http-connector/compare/0.4.0...HEAD

[0.4.0]: https://github.com/getindata/flink-http-connector/compare/0.3.0...0.4.0

[0.3.0]: https://github.com/getindata/flink-http-connector/compare/0.2.0...0.3.0

[0.2.0]: https://github.com/getindata/flink-http-connector/compare/0.1.0...0.2.0

[0.1.0]: https://github.com/getindata/flink-http-connector/compare/dfe9bfeaa73e77b1de14cd0cb0546a925583e23e...0.1.0
