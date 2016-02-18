(function () {
	'use strict';
		
	var thrift = require('thrift'),
	hive = require('./gen-nodejs/TCLIService'),
	ttypes = require('./gen-nodejs/TCLIService_types');
	var util = require('./util');

	/*********************************************************************************/
	/*                                CONFIGURATION                                  */
	/*********************************************************************************/
	
	/*Configuration for connection, read from config.json*/
	var config = JSON.parse(require('fs').readFileSync('./config.json', 'utf8'));

	var connectionConfig = {
		auth : "nosasl",
		timeout : "10000"
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
		client.OpenSession(openSessReq, function (error, response){
			callback(error, response, protocol);	
		});
	}

	/*Close Hive session*/
	function closeSessionThrift (session, callback) {
		var closeSessReq = new ttypes.TCloseSessionReq();
		closeSessReq.sessionHandle = session;
		client.CloseSession(closeSessReq, function(error, response) {
			callback(error,response);
		});
	}
	
	/*Execute HiveQL Statement*/
	function executeStatementThrift (session, statement, callback) {
		var request = new ttypes.TExecuteStatementReq();
		request.sessionHandle = session;
		request.statement = statement;
		request.runAsync = false;
		client.ExecuteStatement(request, function (error, response){
			callback(error,response);
		});
	}
	
	/*Execute fetchRow action on operation result*/
	function fetchRowsThrift (operation, maxRows, callback) {
		var request = new ttypes.TFetchResultsReq();
		request.operationHandle = operation;
		request.orientation = ttypes.TFetchOrientation.FETCH_NEXT;
		request.maxRows = maxRows;
		client.FetchResults(request, function (error, response){
			callback(error,response)
		});
	}
	
	/*********************************************************************************/
	/*                         OBJECT VARIABLES - FUNCTIONS                          */
	/*********************************************************************************/

	/*HiveThriftClient constructor*/
	function HiveThriftClient () {};
	
	/*Connect to Hive database
		- callback : callback(error, session) function
			- error : error
			- session : session opened
	*/
	HiveThriftClient.prototype.connect = function connect (callback) {
		
		logger.trace('connect ...');
		
		/*Create connection and thrift client*/
		connection = thrift.createConnection(config.hiveHost, config.hivePort);
		client = thrift.createClient(hive, connection, {auth : connectionConfig.auth, timeout : connectionConfig.timeout});
		
		/*Handle connection errors*/
		connection.on('error', function(error) {
			logger.trace('connect error : ' + error);
			callback(error,null);
		});

		/*Handle connection success*/
		connection.on('connect', function(){
			
			logger.trace('Connection initialised for ' + config.hiveHost + ':' + config.hivePort);
			
			openSessionThrift(config, function (error, response, protocol) {
				if (error) {
					logger.trace("OpenSession error = " + JSON.stringify(error));
				} else {
					logger.trace("Session opened for user " + config.hiveUser + " with protocol value = " + protocol);
				}
				callback(error,response.sessionHandle);
			});
		});
	};

	/*Disconnect to hive database
		- session : The session, returned by connect, to be closed
		- callback : callback(status) function
			- status : status
	*/
	HiveThriftClient.prototype.disconnect = function disconnect (session, callback) {
		
		logger.trace('disconnect = ' + session);
		
		/*Closing hive session*/
		closeSessionThrift(session, function (status) {
			if (status) {
				logger.trace("disconnect error = " + JSON.stringify(status));
			} else {
				logger.trace('session closed');
			}
			
			/*Handle disconnect success*/
			connection.on('end', function(error) {
				logger.trace('disconnect success');
			});
		
			/*Closing thrift connection*/
			connection.end();
			callback(status);
		});
		
	};
	
	/*Execute a statement
		- session : The session
		- statement : The statement to be executed
		- callback : callback(error, result) function
			- error : error
			- result : result of the executeStatement
	*/
	HiveThriftClient.prototype.executeStatement = function executeStatement (session, statement, callback) {
		logger.trace('executeStatement : ' + statement);
	
		executeStatementThrift(session, statement, function (error, response) {
			if (error) {
				logger.trace("executeStatement error = " + JSON.stringify(error));
				callback(error,null);
			} else if (response.status.statusCode == 3) {
				logger.trace("executeStatement error = " + JSON.stringify(response.status));
				callback(response.status,null);
			} else {
				logger.trace('executeStatement done');
				callback(null,response);
			}
			
		});
	}
	
	/*Execute a fetchRow action on an operation 
		- session : The operation to fetch rows from
		- maxRows : max row number to be fetched
		- callback : callback(error, result) function
			- error : error
			- result : result of the fetchRow
	*/
	HiveThriftClient.prototype.fetchRows = function fetchRows (operation, maxRows, callback) {
		logger.trace('fetchRows on ' + JSON.stringify(operation));
	
		fetchRowsThrift(operation, maxRows, function (error, response) {
			if (error) {
				logger.trace("fetchRows error = " + JSON.stringify(error));
			} else {
				logger.trace('fetchRows done');
			}
			callback(error, response);
		});
	}
	
	/*Execute a select statement and fetch rows fron this operation 
		- session : The operation to fetch rows from
		- statement : The statement to be executed
		- maxRows : max row number to be fetched
		- callback : callback(error, result) function
			- error : error
			- result : result of the fetchRow
	*/
	HiveThriftClient.prototype.executeSelect = function executeSelect (session, selectStatement, maxRows, callback) {
		logger.trace('executeSelect : ' + selectStatement);
	
		HiveThriftClient.prototype.executeStatement(session, selectStatement, function (error, response) {
			if (error) {
				callback(error, response);
			} else {
				HiveThriftClient.prototype.fetchRows(response.operationHandle, maxRows, function (error, response) {
					callback(error, response);
				})
			}
		});
	}
		
	/*TODO : getSchemas*/
	
	module.exports = new HiveThriftClient();
	
})();