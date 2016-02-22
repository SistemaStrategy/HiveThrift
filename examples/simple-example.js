/*********************************************************************************/
/*       If module is installed with npm use the following include :             */
/*       var client = require ('hive-thrift')                                    */

/*********************************************************************************/
var client = require('../index.js');

var bunyan = require('bunyan');

/*********************************************************************************/
/*                                    LOGGER                                     */
/*********************************************************************************/
	
var logger = bunyan.createLogger({
		name: 'HiveThriftWeb',
		stream: process.stdout,
        level: "info"
});

/*********************************************************************************/
/*                                    FUNCTIONS                                  */
/*********************************************************************************/

/*End program function*/
function endProgram(returnVal) {
	logger.info('End of the program, returning ' + returnVal);
	process.exit(returnVal);
}

/*Disconnect function*/
function disconnect(session) {
	client.disconnect(session, function(err, res){
		if(err) {
			logger.error('Disconnection error : ' + err);
			endProgram(1);
		} else {
			logger.info('Disconnection success');
			endProgram(0);
		}	
	});
}

/*Handle error function*/
function handleError(funcName, error, session) {
	logger.error(funcName + " error : " + error);
	disconnect(session);
}

/*********************************************************************************/
/*                                    MAIN                                       */
/*********************************************************************************/

/*This simple test program is querying a table named 'emp' created in the 'test' schema. 
Create these element in order to have results.*/

logger.info('Connecting ...');

/*By default the client API log is silent ... change log level for debug API*/
client.changeLogLevelTrace();

client.connect(function (err, session) {
	if (err) {
		logger.error('Connection error : ' + err);
		endProgram(1);	
	} else {
		logger.info('Connection success');
		logger.info(JSON.stringify(session));

		/*Retrieve database schemas*/
		client.getSchemasNames(session, function (err, resSchema) {
			if(err) {
				handleError('getSchemasNames',err,session);
			} else {
				logger.info("Schemas => " + JSON.stringify(resSchema));
				/*Retrieve a schema name (TABLE_SCHEM[0] = default)*/
				var testSchema = resSchema.TABLE_SCHEM[1];

				/*Retrieve tables for the schema selected*/
				client.getTablesNames(session, testSchema, function (err, resTable) {
					if(err) {
						handleError('getTablesNames',err,session);
					} else {
						logger.info("Tables => " + JSON.stringify(resTable));
						/*Retrieve a table name*/
						var empTable = resTable.TABLE_NAME[1];

						/*Execute select * on the selected table*/
						var selectStatement = 'select * from ' + empTable;
						client.executeSelect(session, selectStatement, function (error, result) {
							if(err) {
								handleError('executeSelect',err,session);
							} else {
								logger.info(selectStatement + " => " + JSON.stringify(result));
								/*Close the session and the connection*/
								disconnect(session);
							}
						});	
					}	
				});
			}
		});
	}
});