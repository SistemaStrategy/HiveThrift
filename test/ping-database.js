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

/*********************************************************************************/
/*                                    MAIN                                       */
/*********************************************************************************/

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

		client.getSchemasNames(session, function (err, resSchema) {
			if(err) {
				logger.error("Error : " + err)
			} else {
				logger.info("Schemas => " + JSON.stringify(resSchema));
			}
			disconnect(session);
		});
	}
});