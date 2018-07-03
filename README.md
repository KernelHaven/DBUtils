# DBUtils

![Build Status](https://jenkins.sse.uni-hildesheim.de/buildStatus/icon?job=KernelHaven_DBUtils)

A utility plugin for [KernelHaven](https://github.com/KernelHaven/KernelHaven).

Utilities for reading an writing SQL tables.

## Usage

Place [`DBUtils.jar`](https://jenkins.sse.uni-hildesheim.de/view/KernelHaven/job/KernelHaven_DBUtils/lastSuccessfulBuild/artifact/build/jar/DBUtils.jar) in the plugins folder of KernelHaven.

This plugin will automatically register the utility classes so that sqlite databases are supported in all places where previously only CSV was.

## Dependencies

This plugin has no additional dependencies other than KernelHaven.

## License

This plugin is licensed under the [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0.html).
