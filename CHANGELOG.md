# Changelog

## [Unreleased]

- Implement [HttpSink](src/main/java/com/getindata/connectors/http/sink/HttpSink.java) deriving from [AsyncSinkBase](https://cwiki.apache.org/confluence/display/FLINK/FLIP-171%3A+Async+Sink) introduced in Flink 1.15.
- Add support for Table API in HttpSink in the form of [HttpDynamicSink](src/main/java/com/getindata/connectors/http/table/HttpDynamicSink.java). 

## [0.1.0] - 2022-05-26

-   Implement basic support for Http connector for Flink SQL

[Unreleased]: https://github.com/getindata/flink-http-connector/compare/0.1.0...HEAD

[0.1.0]: https://github.com/getindata/flink-http-connector/compare/dfe9bfeaa73e77b1de14cd0cb0546a925583e23e...0.1.0
