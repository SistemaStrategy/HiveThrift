var client = require('./lib/HiveThriftClient');
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
/*                                    MAIN                                       */
/*********************************************************************************/
function endProgram(returnVal) {
	logger.info('End of the program, returning ' + returnVal);
	process.exit(returnVal);
}

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

logger.info('Connecting ...');

client.connect(function (err, session) {
	
	if(err) {
		logger.error('Connection error : ' + err);
		endProgram(1);	
	} else {
		logger.info('Connection success');
		logger.info(JSON.stringify(session));

		client.getSchemasNames(session, function (err, resSchema){

			logger.info("Schemas => " + JSON.stringify(resSchema));
			var testSchema = resSchema.TABLE_SCHEM[1];
			client.getTablesNames(session, testSchema, function (err, resTable){

				client.changeLogLevelSilent();
				logger.info("Tables => " + JSON.stringify(resTable));
				var empTable = resTable.TABLE_NAME[1];
				client.getColumns(session, testSchema, empTable, function (err, resCol){

					logger.info("Columns for " + testSchema + "." + empTable + " => " + JSON.stringify(resCol));
					disconnect(session);

				});			
			});
		});
	}
	
});