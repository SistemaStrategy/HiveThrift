var client = require('./lib/HiveThriftClient');
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

var date = new Date();
var time1, time2;

client.connect(function (err, session) {
	
	if(err) {
		logger.error('Connection error : ' + err);
		endProgram(1);	
	} else {
		logger.info('Connection success');
		logger.info(JSON.stringify(session));
		time1 = date.getTime();
	
		client.executeSelect(session, "select * from test.emp where emp.id > 1000", 50, function (err, res) {
			logger.info("select * from test.emp where emp.id > 1000 => " + JSON.stringify(res));
			disconnect(session);
		});
	}
	
});