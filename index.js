/**************/
/*Require part*/
/**************/

var thrift = require('thrift'),
    Hive = require('./lib/gen-nodejs/TCLIService'),
    ttypes = require('./lib/gen-nodejs/TCLIService_types');
	
/********************/
/*Hive configuration*/
/********************/

var hiveHost = "vm-cluster2-node1",
	hivePort = 10000,
	hiveUser = 'root',
	hivePassword = 'root';
	
/*****************/
/*Client creation*/
/*****************/

var connection = thrift.createConnection(hiveHost, hivePort),
    client = thrift.createClient(Hive, connection);
	
/***********/
/*Main code*/
/***********/

connection.on('error', function(err) {
  console.error(err);
});

connection.on('connect', function(){
	console.log(connection);
	/*Open session*/
	openSessReq = new ttypes.TOpenSessionReq();
	openSessReq.client_protocol = ttypes.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8;
	openSessReq.username = hiveUser;
	openSessReq.password = hivePassword;
	var session = client.OpenSession(openSessReq);
	console.log("Session : " + session);
	/*Close session*/
});

/*Close connection ?*/


