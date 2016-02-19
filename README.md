# HiveThrift

HiveThrift is a nodejs application for fast querying of Hive using Thrift API. 

## Install
To install HiveThrift you need to checkout the sources: `git clone git://github.com/SistemaStrategy/HiveThrift.git`

## Getting Started
* Install **nodejs** and **npm**
* Execute the following commands:

```
npm install
node index.js | ./node_modules/bunyan/bin/bunyan

```
Bunyan is a simple and fast JSON logging library for node.js service. A bunyan CLI tool is provided (**./node_modules/bunyan/bin/bunyan**) for pretty-printing bunyan logs and for filtering.
To learn more about Bunyan click [here](https://github.com/trentm/node-bunyan).

## Hive configuration
In order to query the desired Hive instance, modify the Hive configuration stored in **config.json** file. The host must be pointing to the **HiveServer2** instance.
