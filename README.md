# DBUtils

![Build Status](https://jenkins-2.sse.uni-hildesheim.de/buildStatus/icon?job=KH_DBUtils)

A utility plugin for [KernelHaven](https://github.com/KernelHaven/KernelHaven).

Utilities for reading an writing tables in [SQLite databases](https://sqlite.org/).

## Usage

Place [`DBUtils.jar`](https://jenkins-2.sse.uni-hildesheim.de/view/KernelHaven/job/KH_DBUtils/lastSuccessfulBuild/artifact/build/jar/DBUtils.jar) in the plugins folder of KernelHaven.

This plugin will automatically register the utility classes so that SQLite databases (`*.sqlite`) are supported in all places where previously only CSV was.

## Dependencies

This plugin has no additional dependencies other than KernelHaven.

## License

This plugin is licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0.html).

## Used Libraries

The following libraries are used (and bundled in `lib/`) by this plugin:

| Library | Version | License |
|---------|---------|---------|
| [SQLite JDBC](https://github.com/xerial/sqlite-jdbc) | [3.23.1](https://bitbucket.org/xerial/sqlite-jdbc/downloads/sqlite-jdbc-3.23.1.jar) | [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0.html) |
