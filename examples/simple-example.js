/*********************************************************************************/
/*       If module is installed with npm use the following include :             */
/*       var client = require ('hive-thrift')                                    */
/*********************************************************************************/
var client = require('../index.js');
var util = require('../src/util.js');
var bunyan = require('bunyan');

/*********************************************************************************/
/*                                    LOGGER                                     */
/*********************************************************************************/
	
var logger = bunyan.createLogger({
		name: 'HiveThrifSimpleExample',
		stream: process.stdout,
        level: "info"
});

/*********************************************************************************/
/*                                    FUNCTIONS                                  */
/*********************************************************************************/
/*Handle error function*/
function handleError(funcName, error, session) {
	logger.error(funcName + " error : " + JSON.stringify(error));
	util.disconnect(session);
}

/*Get a specific element*/
function getByValue(attrName, attrValue, object) {
	for(var i = 0 ; i < object[attrName].length; i++) {
		if(object[attrName][i] == attrValue) {
			return attrValue;
		}
	}
	return null;
}

/*********************************************************************************/
/*                                    MAIN                                       */
/*********************************************************************************/

/*This simple test program is querying a table named 'emp' created in the 'test' schema. 
Create these element in order to have results.*/
var schemaName = 'test';
var tableName = 'emp';

logger.info('Connecting ...');

/*By default the client API log is silent ... change log level for debug API*/
client.changeLogLevelTrace();

client.connect(function (err, session) {
	if (err) {
		logger.error('Connection error : ' + JSON.stringify(err));
		util.endProgram(1);	
	} else {
		logger.info('Connection success');
		logger.info(JSON.stringify(session));

		/*Retrieve database schemas*/
		client.getSchemasNames(session, function (err, resSchema) {
			if(err) {
				handleError('getSchemasNames',err,session);
			} else {
				logger.info("Schemas => " + JSON.stringify(resSchema));
				if(!getByValue('TABLE_SCHEM',schemaName,resSchema)) {
					logger.error('Schema ' + schemaName + ' not existing ... ');
					util.disconnect(session);
				}

				/*Retrieve tables for schemaName*/
				client.getTablesNames(session, schemaName, function (err, resTable) {
					if(err) {
						handleError('getTablesNames',err,session);
					} else {
						logger.info("Tables => " + JSON.stringify(resTable));
						if(!getByValue('TABLE_NAME',tableName,resTable)) {
							logger.error('Table ' + tableName + ' not existing ... ');
							util.disconnect(session);
						}
						/*Execute select * on tableName*/
						var selectStatement = 'select * from ' + schemaName + '.' + tableName;
						client.executeSelect(session, selectStatement, function (error, result) {
							if(err) {
								handleError('executeSelect',err,session);
							} else {
								logger.info(selectStatement + " => " + JSON.stringify(result));
								/*Close the session and the connection*/
								util.disconnect(session);
							}
						});	
					}	
				});
			}
		});
	}
});