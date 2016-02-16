# HiveThrift

HiveThrift is a nodejs application for fast querying of Hive using Thrift API. 

## Install
To install HiveThrift you need to checkout the sources: `git clone git://github.com/SistemaStrategy/HiveThrift.git`

## Getting Started
* Install **nodejs** and **npm**
* Execute the following commands:

```
npm install &&
node index.js
```

## Hive configuration
In order to query the desired Hive instance, modify the `Hive configuration` part inside **index.js**. The host must be pointing to the **HiveServer2** instance.