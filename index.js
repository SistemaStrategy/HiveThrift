var client = require('./lib/Client');
var bunyan = require('bunyan');

/*********************************************************************************/
/*                                    LOGGER                                     */
/*********************************************************************************/
	
var logger = bunyan.createLogger({
		name: 'HiveThriftWeb',
		stream: process.stderr,
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
		
		client.executeStatement(session, "select * from test.emp", function (err, response) {
			
			if(err) {
				logger.error('ExecuteStatement error = ' + err);
				disconnect(session);
			} else {
				client.fetchRows(response.operationHandle, 50, function(err, response) {
					logger.info(JSON.stringify(response));
					disconnect(session);
				})
			}
			
		});
	}
	
});