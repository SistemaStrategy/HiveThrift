(function () {
	'use strict';
	var bunyan = require('bunyan');

	function Util () {};
	
	/*Create parametrized log by the client ?*/
	var log = bunyan.createLogger({
		name: 'HiveThriftLibrary',
		streams: [
			{
				level: 'trace',
				type: 'rotating-file',
				path: './logs/HiveThriftLibrary.trace',
				period: '1d',   // daily rotation
				count: 10        // keep 10 back copies
			}
		]
	});
	
	Util.prototype.mainLogger = log;

	module.exports = new Util();
})();