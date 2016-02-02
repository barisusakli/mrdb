
'use strict';



var mrdb = require('./index');
var async = require('async');


mrdb.init({}, function(err) {
	if (err) {
		console.log(err);
		process.exit();
	}

	benchHash(function(err, results) {
		console.log(results);
		process.exit();
	});
});

process.elapsedTimeSince = function(start) {
	var diff = process.hrtime(start);
	return diff[0] * 1e3 + diff[1] / 1e6;
};

function benchHash(callback) {
	var arr = [];
	var count = 10000;
	for(var i=0; i<count; ++i) {
		arr.push({value: i, score: Date.now()});
	}
	var start = process.hrtime();
	var min = 10000;
	var max = 0;
	var now;
	var took;
	async.eachSeries(arr, function(item, next) {
		now = process.hrtime();
		mrdb.hmset('hash:'+ item.value, item, function(err) {
			if (err) {
				return callback(err);
			}
			took = process.elapsedTimeSince(now);
			if (took > max) {
				max = took;
			} else if (took < min) {
				min = took;
			}
			next();
		});
	}, function(err) {
		if (err) {
			return callback(err);
		}
		var totalTime = process.elapsedTimeSince(start);
		var results = {
			ops: (count / totalTime) * 1000,
			avgPerQuery: totalTime / count,
			min: min,
			max: max,
			total: totalTime,

		};
		callback(null, results);
	});
}

function benchZset(callback) {
	var arr = [];
	var count = 10000;
	for(var i=0; i<count; ++i) {
		arr.push({value: i, score: Date.now()});
	}
	var start = process.hrtime();
	var min = 10000;
	var max = 0;
	var now;
	var took;
	async.eachSeries(arr, function(item, next) {
		now = process.hrtime();
		mrdb.zadd('sortedSetBench', item.score, item.value, function(err) {
			if (err) {
				return callback(err);
			}
			took = process.elapsedTimeSince(now);
			if (took > max) {
				max = took;
			} else if (took < min) {
				min = took;
			}
			next();
		});
	}, function(err) {
		if (err) {
			return callback(err);
		}
		var totalTime = process.elapsedTimeSince(start);
		var results = {
			ops: (count / totalTime) * 1000,
			avgPerQuery: totalTime / count,
			min: min,
			max: max,
			total: totalTime,

		};
		callback(null, results);
	});


}








