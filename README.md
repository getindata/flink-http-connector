# Apache Flink HTTP Connector

This repository contains the official Apache Flink HTTP connector.

## Apache Flink

Apache Flink is an open source stream processing framework with powerful stream- and batch-processing capabilities.

Learn more about Flink at [https://flink.apache.org/](https://flink.apache.org/)

## Building the Apache Flink HTTP Connector from Source

Prerequisites:

* Unix-like environment (we use Linux, Mac OS X)
* Git
* Maven (we recommend version 3.8.6)
* Java 11

```
git clone https://github.com/apache/flink-connector-http.git
cd flink-connector-http
mvn clean package -DskipTests
```

The resulting jars can be found in the `target` directory of the respective module.

## Developing Flink

The Flink committers use IntelliJ IDEA to develop the Flink codebase.
We recommend IntelliJ IDEA for developing projects that involve Scala code.

Minimal requirements for an IDE are:
* Support for Java and Scala (also mixed projects)
* Support for Maven with Java and Scala

### IntelliJ IDEA

The IntelliJ IDE supports Maven out of the box and offers a plugin for Scala development.

* IntelliJ download: [https://www.jetbrains.com/idea/](https://www.jetbrains.com/idea/)
* IntelliJ Scala Plugin: [https://plugins.jetbrains.com/plugin/?id=1347](https://plugins.jetbrains.com/plugin/?id=1347)

Check out our [Setting up IntelliJ](https://nightlies.apache.org/flink/flink-docs-master/flinkDev/ide_setup.html#intellij-idea) guide for details.

## Support

Don’t hesitate to ask!

Contact the developers and community on the [mailing lists](https://flink.apache.org/community.html#mailing-lists) if you need any help.

[Open an issue](https://issues.apache.org/jira/browse/FLINK) if you found a bug in Flink.

## Documentation

The documentation of Apache Flink is located on the website: [https://flink.apache.org](https://flink.apache.org)
or in the `docs/` directory of the source code.

## Fork and Contribute

This is an active open-source project. We are always open to people who want to use the system or contribute to it.
Contact us if you are looking for implementation tasks that fit your skills.
This article describes [how to contribute to Apache Flink](https://flink.apache.org/contributing/how-to-contribute.html).

## About

Apache Flink is an open source project of The Apache Software Foundation (ASF).
The Apache Flink project originated from the [Stratosphere](http://stratosphere.eu) research project.
