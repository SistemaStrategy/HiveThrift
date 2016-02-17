/**************/
/*Require part*/
/**************/

var thrift = require('thrift'),
    hive = require('./lib/gen-nodejs/TCLIService'),
    ttypes = require('./lib/gen-nodejs/TCLIService_types');
	
/********************/
/*Hive configuration, create config.json file*/
/********************/

var config = {
	hiveHost : "vm-cluster2-node1",
	hivePort : 10000,
	hiveUser: 'root',
	hivePassword : 'root',
	auth : "nosasl",
	timeout : "10000",
}

/*
var config = JSON.parse(yield new Promise(function (resolve, reject) {
    fs.readFile('./config.json', function (err, buf) {
      if (err) {
        reject(err);
      } else {
        resolve(buf);
      }
	});
}));*/
	
/************************************/
/*Library definition, to externalize*/
/************************************/
/*Open Hive session*/
function openSession(config, callback) {
	openSessReq = new ttypes.TOpenSessionReq();
	openSessReq.username = config.hiveUser;
	openSessReq.password = config.hivePassword;
	openSessReq.client_protocol = ttypes.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V7;
	//openSessReq.configuration = options;
	console.log(openSessReq);
	client.OpenSession(openSessReq, function (err, result){
		if (err) {
			console.log("OpenSession erreur : " + err);
		} else {
			console.log("OpenSession status : " + result.status.statusCode);
			callback(result.sessionHandle);
		}
	});
}

/*Close Hive session*/
function closeSession(session) {
	client.CloseSession(session, function(err, result) {
		if (err) {
			console.log("closeSession :  erreur = " + error);
		} else {
			console.log('Session closed');
			connection.end();
			console.log('Connection closed');
		}
	});
}

/*Execute HiveQL Statement*/
function executeStatement (session, statement, callback) {
	request = new ttypes.TExecuteStatementReq();
	request.sessionHandle = session;
	request.statement = statement;
	request.runAsync = false;
	client.ExecuteStatement(request, function (err, result){
		if (err) {
			console.log("ExecuteStatement erreur : " + err);
		} else {
			console.log("ExecuteStatement status : " + result.status.statusCode);
			callback(result.operationHandle);
		}
	});
}

function fetchRows (operation, maxRows, callback) {
	request = new ttypes.TFetchResultsReq();
	request.operationHandle = operation;
	request.orientation = ttypes.TFetchOrientation.FETCH_NEXT;
	request.maxRows = maxRows;
	client.FetchResults(request, function (err, result){
		if (err) {
			console.log("FetchResults erreur : " + err);
		} else {
			console.log("FetchResults status : " + result.status.statusCode);
			callback(result);
		}
	});
}

/***********/
/*Main code*/
/***********/

var connection = thrift.createConnection(config.hiveHost, config.hivePort);
var client = thrift.createClient(hive, connection, {auth : config.auth, timeout : config.timeout});

connection.on('error', function(err) {
	console.log('Connection error : ' + err);
	/*Close the program with error code*/
	process.exit(1)
});

connection.on('connect', function(){
	console.log("OpenSession call");
	openSession(config, function (session) {
		console.log('Inside callback ... ');
		executeStatement(session, "use test", function (operation) {});
		executeStatement(session, "select * from emp", function (operation) {
			fetchRows(operation, 50, function (fetchResult) {
				console.log(JSON.stringify(fetchResult));
				endProgram(session);
			});
		});
	});
});

function endProgram(session) {
	client.CloseSession(session);
	console.log("Program done");
	process.exit(0);
}