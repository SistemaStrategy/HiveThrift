# HiveThrift
HiveThrift is a node API for fast querying of Hive using Thrift middleware. 

##Prerequisite
* NodeJS >= 4.3.0
* npm 
* git (installation from sources)

## Install 
###Install from sources
To install HiveThrift you need to checkout the sources: `git clone git://github.com/SistemaStrategy/HiveThrift.git`

###Install using npm
To install HiveThrift using npm, use the following command : 
```
npm install hive-thrift
```
If you're using a Vagrant box, use the --no-bin-links argument
```
npm install hive-thrift --no-bin-links
```

## Getting Started
### From sources
* Update the config.json file with your settings
* Execute the following commands:
```
npm install 
npm test 
```
Once your database settings are corrects, use the **examples** folder to create a program and run it like the following 
```
node your_program.js | ./node_modules/bunyan/bin/bunyan
```
Bunyan is a simple and fast JSON logging library for node.js service. A bunyan CLI tool is provided (**./node_modules/bunyan/bin/bunyan**) for pretty-printing bunyan logs and for filtering.
To learn more about Bunyan click [here](https://github.com/trentm/node-bunyan).

### From npm
* Create a config.json file in the root directory of your project like the config.json present [here](https://github.com/SistemaStrategy/HiveThrift/blob/master/config.json)
* Write your node program like [here](https://github.com/SistemaStrategy/HiveThrift/blob/master/examples/simple-example.js) and don't forget to use the library by using the following import
```
var client = require('hive-thrift');
```

### Example code
Some API uses are shown in the **examples** folder. Don't forget to change the **require** part if you're using npm install ... 

## Hive configuration
The Hive configuration is stored in a config.json file present in the root directory of the project. The host must be pointing to the **HiveServer2** instance.


## API documentation 
TODO
