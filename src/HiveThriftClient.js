(function () {
	'use strict';
		
	var thrift = require('thrift'),
		hive = require('../lib/gen-nodejs/TCLIService'),
		ttypes = require('../lib/gen-nodejs/TCLIService_types'),
		bunyan = require('bunyan');

	/*********************************************************************************/
	/*                                CONFIGURATION                                  */
	/*********************************************************************************/
	
	/*Configuration for connection, read from config.json*/
	var config = JSON.parse(require('fs').readFileSync('./config.json', 'utf8'));

	var connectionConfig = {
		auth : "nosasl",
		timeout : "10000"
	}

	/*********************************************************************************/
	/*                         LOCAL VARIABLES - FUNCTIONS                           */
	/*********************************************************************************/
	
	/*Create library logger*/
	var logger = bunyan.createLogger({
		name: 'HiveThriftLibrary',
		streams: [
			{
				level: 'info',
				type: 'rotating-file',
				path: './logs/HiveThriftLibrary.trace',
				period: '1d',   // daily rotation
				count: 10        // keep 10 back copies
			}
		]
	});
	
	/*Change log level to TRACE*/
	HiveThriftClient.prototype.changeLogLevelTrace = function changeLogLevelTrace () {
		logger.levels(0,'trace');
	}

	/*Change log level to SILENT*/
	HiveThriftClient.prototype.changeLogLevelSilent = function changeLogLevelSilent () {
		logger.levels(0,'info');
	}

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
	
	/*Execute getSchemas action */
	function getSchemasThrift (session, callback) {
		var request = new ttypes.TGetSchemasReq();
		request.sessionHandle = session;
		client.GetSchemas(request, function (error, response){
			callback(error,response)
		});
	}
	
	/*Execute getTables action */
	function getTablesThrift (session, schemaName, callback) {
		var request = new ttypes.TGetTablesReq();
		request.sessionHandle = session;
		request.schemaName = schemaName;
		client.GetTables(request, function (error, response){
			callback(error,response)
		});
	}
	
	/*Execute getColumns action */
	function getColumnsThrift (session, schemaName, tableName, callback) {
		var request = new ttypes.TGetColumnsReq();
		request.sessionHandle = session;
		request.schemaName = schemaName;
		request.tableName = tableName;
		client.GetColumns(request, function (error, response){
			callback(error,response)
		});
	}
	
	/*Execute GetResultSetMetadata action */
	function getResultSetMetadataThrift (operation, callback) {
		var request = new ttypes.TGetResultSetMetadataReq();
		request.operationHandle = operation;
		client.GetResultSetMetadata(request, function (error, response){
			callback(error,response)
		});
	}

	/***************************/
	/* Tansformation of results*/
	/***************************/

	/*Get TColumnValue from TTypeID, used to retrieve data from fetch (TColumnValue) with metadata knowledge (TTypeID)*/
	function getReverseTColumn (numericValue) {
		switch (numericValue) { 
			case ttypes.TTypeId.BOOLEAN_TYPE : return 'boolVal';
			case ttypes.TTypeId.TINYINT_TYPE : return 'byteVal';
			case ttypes.TTypeId.SMALLINT_TYPE : return 'i16Val';
			case ttypes.TTypeId.INT_TYPE : return 'i32Val';
			case ttypes.TTypeId.BIGINT_TYPE : return 'i64Val';
			case ttypes.TTypeId.FLOAT_TYPE : return 'doubleVal';
			case ttypes.TTypeId.DOUBLE_TYPE : return 'doubleVal';
			case ttypes.TTypeId.STRING_TYPE : return 'stringVal';
			case ttypes.TTypeId.TIMESTAMP_TYPE : return 'i64Val';
			case ttypes.TTypeId.BINARY_TYPE : return 'stringVal';
			case ttypes.TTypeId.ARRAY_TYPE : return 'stringVal';
			case ttypes.TTypeId.MAP_TYPE : return 'stringVal';
			case ttypes.TTypeId.STRUCT_TYPE : return 'stringVal';
			case ttypes.TTypeId.UNION_TYPE : return 'stringVal';
			case ttypes.TTypeId.USER_DEFINED_TYPE : return 'stringVal';
			case ttypes.TTypeId.DECIMAL_TYPE : return 'stringVal';
			case ttypes.TTypeId.NULL_TYPE : return 'stringVal';
			case ttypes.TTypeId.DATE_TYPE : return 'stringVal';
			case ttypes.TTypeId.VARCHAR_TYPE : return 'stringVal';
			case ttypes.TTypeId.CHAR_TYPE : return 'stringVal';
		    case ttypes.TTypeId.INTERVAL_YEAR_MONTH_TYPE : return 'stringVal';
	        case ttypes.TTypeId.INTERVAL_DAY_TIME_TYPE : return 'stringVal';
			default : return null;
		}
	}

	/*Fix FetchRowsThrift limitation*/
	function getKeyValueRows (operation, callback) {
		getResultSetMetadataThrift(operation, function (error, responseMeta){
			if(error) {
				callback(error,null);
			} else {
				fetchRowsThrift(operation, 50, function (error, responseFetch) {
					if(error) {
						callback(error,null);
					} else {
						var result = new Object();
						var metaColumns = responseMeta.schema.columns;
						var rowColumns = responseFetch.results.columns;
						var currentMeta, currentRow;
						var type = '';
						for(var i = 0 ; i < metaColumns.length; i++) {
							currentMeta = metaColumns[i];
							currentRow = rowColumns[i];
							type = getReverseTColumn(currentMeta.typeDesc.types[0].primitiveEntry.type);
							logger.trace("----- getKeyValueRows ----- columnName = " + currentMeta.columnName + " position = " + i 
							+ " type = " + currentMeta.typeDesc.types[0].primitiveEntry.type );
							logger.trace("----- getKeyValueRows ----- value = " + JSON.stringify(currentRow[type].values));
							result[currentMeta.columnName] = currentRow[type].values;
						}
						callback(error, result);
					}
				});
			}
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
			- session : opened session
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
		- session : The opened session
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
	
	/*Execute a statement, using the raw Thrift Hive API
		- session : The opened session
		- statement : The statement to be executed
		- callback : callback(error, result) function
			- error : error
			- result : result of the rawExecuteStatement
	*/
	HiveThriftClient.prototype.rawExecuteStatement = function rawExecuteStatement (session, statement, callback) {
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
	
	/*Execute a fetchRow action on an operation, using the raw Thrift Hive API
		- operation : The operation to fetch rows from
		- maxRows : max row number to be fetched
		- callback : callback(error, result) function
			- error : error
			- result : result of the rawFetchRows
	*/
	HiveThriftClient.prototype.rawFetchRows = function rawFetchRows (operation, maxRows, callback) {
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
	
	/*Execute a select statement
		- session : The operation to fetch rows from
		- statement : The statement to be executed
		- maxRows : max row number to be fetched
		- callback : callback(error, result) function
			- error : error
			- result : result of the executeSelect
	*/
	HiveThriftClient.prototype.executeSelect = function executeSelect (session, selectStatement, callback) {
		logger.trace('executeSelect : ' + selectStatement);
	
		HiveThriftClient.prototype.rawExecuteStatement(session, selectStatement, function (error, response) {
			if (error) {
				logger.trace("executeSelect error = " + JSON.stringify(error));
				callback(error, response);
			} else {
				logger.trace('executeSelect done, fetching rows');
				getKeyValueRows(response.operationHandle, function(error, response){
					callback(error,response);
				});
			}
		});
	}
	
	/*Retrieve schema informations
		- session : The opened session
		- callback : callback(error, result) function
			- error : error
			- result : result of the fetchRow
	*/
	HiveThriftClient.prototype.getSchemasNames = function getSchemasNames (session, callback) {
		logger.trace('getSchemas : ' + JSON.stringify(session));
	
		getSchemasThrift(session, function (error, response) {
			if (error) {
				logger.trace("getSchemas error = " + JSON.stringify(error));
				callback(error, response);
			} else {
				logger.trace('getSchemas done, fetching rows');
				getKeyValueRows(response.operationHandle, function(error, response){
					callback(error,response);
				});
			}
		});
	}
	
	/*Retrieve tables informations 
		- session : The opened session
		- schemaName : The schema name
		- callback : callback(error, result) function
			- error : error
			- result : result of the fetchRow
	*/
	HiveThriftClient.prototype.getTablesNames = function getTablesNames (session, schemaName, callback) {
		logger.trace('getTablesNames for schema ' + schemaName);
	
		getTablesThrift(session, schemaName, function (error, response) {
			if (error) {
				logger.trace("getTablesNames error = " + JSON.stringify(error));
				callback(error, response);
			} else {
				logger.trace('getTablesNames done, fetching rows');
				getKeyValueRows(response.operationHandle, function(error, response){
					callback(error,response);
				});
			}
		});
	}
	
	/*Retrieve columns of a table  
		- session : The opened session
		- schemaName : The schema name
		- tableName : The table name 
		- callback : callback(error, result) function
			- error : error
			- result : result of the fetchRow
	*/
	HiveThriftClient.prototype.getColumns = function getColumns (session, schemaName, tableName, callback) {
		logger.trace('getColumns for schema ' + schemaName + ' and table ' + tableName);
	
		getColumnsThrift(session, schemaName, tableName, function (error, response) {
			if (error) {
				callback(error, response);
			} else {
				logger.trace('getColumns done, fetching rows');
				getKeyValueRows(response.operationHandle, function(error, response){
					callback(error,response);
				});
			}
		});
	}
			
	module.exports = new HiveThriftClient();
	
})(); 