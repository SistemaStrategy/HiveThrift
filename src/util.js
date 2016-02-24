/*********************************************************************************/
/*                                    FUNCTIONS                                  */
/*********************************************************************************/
var client = require('../index.js');
var bunyan = require('bunyan');

var logger = bunyan.createLogger({
		name: 'HiveThriftUtil',
		stream: process.stdout,
        level: "info"
});

/*End program function without disconnect*/
exports.endProgram = function (returnVal) {
	logger.info('End of the program, returning ' + returnVal);
	process.exit(returnVal);
}

/*Disconnect and end program function*/
exports.disconnect = function (session) {
	client.disconnect(session, function(err, res){
		if(err) {
			logger.error('Disconnection error : ' + JSON.stringify(err));
			endProgram(1);
		} else {
			logger.info('Disconnection success');
			exports.endProgram(0);
		}	
	});
}