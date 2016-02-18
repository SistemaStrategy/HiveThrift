(function () {
	'use strict';
		
	var thrift = require('thrift'),
	hive = require('./gen-nodejs/TCLIService'),
	ttypes = require('./gen-nodejs/TCLIService_types');
	var util = require('./util');

	/*********************************************************************************/
	/*                                CONFIGURATION                                  */
	/*********************************************************************************/
	
	/*Configuration for connection, thrift client and hive session, to separate ? Yes in file !*/
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

	/*********************************************************************************/
	/*                         LOCAL VARIABLES - FUNCTIONS                           */
	/*********************************************************************************/
	
	/*Create client specific child logger*/
	var logger = util.mainLogger.child({entity: 'Client object'});
	var client, connection;

	/*Open Hive session*/
	function openSessionThrift (config, callback) {
		var protocol = ttypes.TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V7
		var openSessReq = new ttypes.TOpenSessionReq();
		openSessReq.username = config.hiveUser;
		openSessReq.password = config.hivePassword;
		openSessReq.client_protocol = protocol;
		//openSessReq.configuration = options;
		
		client.OpenSession(openSessReq, function (err, res){
			
			if (err) {
				logger.trace("OpenSession error = " + err);
			} else {
				logger.trace("Session opened for user " + config.hiveUser + " with protocol value = " + protocol);
			}
			callback(err, res.sessionHandle);
			
		});
		
	}

	/*Close Hive session*/
	function closeSessionThrift (session, callback) {
		var closeSessReq = new ttypes.TCloseSessionReq();
		closeSessReq.sessionHandle = session;
		client.CloseSession(closeSessReq, function(err, res) {
			callback(err,res);
		});
	}
	
	/*Execute HiveQL Statement*/
	function executeStatementThrift (session, statement, callback) {
		var request = new ttypes.TExecuteStatementReq();
		request.sessionHandle = session;
		request.statement = statement;
		request.runAsync = false;
		client.ExecuteStatement(request, function (err, res){
			callback(err,res);
		});
	}
	
	/*Execute fetchRow action on operation result*/
	function fetchRowsThrift (operation, maxRows, callback) {
		var request = new ttypes.TFetchResultsReq();
		request.operationHandle = operation;
		request.orientation = ttypes.TFetchOrientation.FETCH_NEXT;
		request.maxRows = maxRows;
		client.FetchResults(request, function (err, res){
			callback(err,res)
		});
	}
	
	/*********************************************************************************/
	/*                         OBJECT VARIABLES - FUNCTIONS                          */
	/*********************************************************************************/

	/*Client constructor*/
	function Client () {};
	
	/*Connect function for the Client object
		- callback : callback(err, session) function
			- err : error
			- session : session opened
	*/
	Client.prototype.connect = function connect (callback) {
		
		logger.trace('connect ...');
		
		/*Create connection and thrift client*/
		connection = thrift.createConnection(config.hiveHost, config.hivePort);
		client = thrift.createClient(hive, connection, {auth : config.auth, timeout : config.timeout});
		
		/*Handle connection errors*/
		connection.on('error', function(err) {
			logger.trace('connect error : ' + err);
			callback(err,null);
		});

		/*Handle connection success*/
		connection.on('connect', function(){
			
			logger.trace('Connection initialised for ' + config.hiveHost + ':' + config.hivePort);
			
			openSessionThrift(config, function (err, session) {
				callback(err,session);
			});
		});
	};

	/*Connect function for the Client object
		- session : The session, returned by connect, to be closed
		- callback : callback(err, result) function
			- err : error
			- result : result of the disconnect function
	*/
	Client.prototype.disconnect = function disconnect (session, callback) {
		
		logger.trace('disconnect = ' + session);
	
		closeSessionThrift(session, function (err, res) {
			if (err) {
				logger.trace("disconnect error = " + err);
			} else {
				logger.trace('Session closed');
				connection.end();
				logger.trace('Connection closed');
			}
			callback(err,res);
		});
		
	};
	
	/*Execute a statement
		- session : The session
		- statement : The statement to be executed
		- callback : callback(err, result) function
			- err : error
			- result : result of the executeStatement
	*/
	Client.prototype.executeStatement = function execStatement (session, statement, callback) {
		logger.trace('executeStatement : ' + statement);
	
		executeStatementThrift(session, statement, function (err, res) {
			if (err) {
				logger.trace("executeStatement error = " + err);
			} else {
				logger.trace('executeStatement done');
			}
			callback(err,res);
		});
	}
	
	/*Execute a fetchRow action on an operation 
		- session : The operation to fetch rows from
		- maxRows : max row number to be fetched
		- callback : callback(err, result) function
			- err : error
			- result : result of the fetchRow
	*/
	Client.prototype.fetchRows = function fetchRows (operation, maxRows, callback) {
		logger.trace('fetchRows on ' + JSON.stringify(operation));
	
		fetchRowsThrift(operation, maxRows, function (err, res) {
			if (err) {
				logger.trace("fetchRows error = " + err);
			} else {
				logger.trace('fetchRows done');
			}
			callback(err,res);
		});
	}
	
	/*TODO : mix executeStatement and fetchRows to have a nice function*/
	
	
	module.exports = new Client();
	
})();