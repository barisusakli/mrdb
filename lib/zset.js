"use strict";

var async = require('async');

module.exports = function(db, module) {
	var helpers = require('./helpers');

	module.zadd = function(key, score, value, callback) {
		callback = callback || helpers.noop;
		if (!key) {
			return callback();
		}

		if (Array.isArray(key)) {
			return module.sortedSetsAdd(key, score, value, callback);
		}

		if (Array.isArray(score) && Array.isArray(value)) {
			return sortedSetAddBulk(key, score, value, callback);
		}

		value = helpers.valueToString(value);

		db.collection('zset').update({k: key, v: value}, {$set: {s: parseInt(score, 10)}}, {upsert: true, w: 1}, function(err) {
			callback(err);
		});
	};

	function sortedSetAddBulk(key, scores, values, callback) {
		if (!scores.length || !values.length) {
			return callback();
		}
		if (scores.length !== values.length) {
			return callback(new Error('[[error:invalid-data]]'));
		}

		values = values.map(helpers.valueToString);

		var bulk = db.collection('zset').initializeUnorderedBulkOp();

		for(var i=0; i<scores.length; ++i) {
			bulk.find({k: key, v: values[i]}).upsert().updateOne({$set: {s: parseInt(scores[i], 10)}});
		}

		bulk.execute(function(err) {
			callback(err);
		});
	}

	module.sortedSetsAdd = function(keys, score, value, callback) {
		callback = callback || helpers.noop;
		if (!Array.isArray(keys) || !keys.length) {
			return callback();
		}
		value = helpers.valueToString(value);

		var bulk = db.collection('zset').initializeUnorderedBulkOp();

		for(var i=0; i<keys.length; ++i) {
			bulk.find({k: keys[i], v: value}).upsert().updateOne({$set: {s: parseInt(score, 10)}});
		}

		bulk.execute(function(err) {
			callback(err);
		});
	};

	module.zrem = function(key, value, callback) {
		function done(err) {
			callback(err);
		}
		callback = callback || helpers.noop;
		if (!key) {
			return callback();
		}

		if (Array.isArray(key)) {
			value = helpers.valueToString(value);
			db.collection('zset').remove({k: {$in: key}, v: value}, done);
		} else if (Array.isArray(value)) {
			value = value.map(helpers.valueToString);
			db.collection('zset').remove({k: key, v: {$in: value}}, done);
		} else {
			value = helpers.valueToString(value);
			db.collection('zset').remove({k: key, v: value}, done);
		}
	};

	module.zremrangebyscore = function(key, min, max, callback) {
		callback = callback || helpers.noop;
		if (!Array.isArray(key)) {
			key = [key];
		}

 		if (!key.length) {
 			return callback();
 		}

		db.collection('zset').remove({k: {$in: key}, s: {$lte: max, $gte: min}}, function(err) {
			callback(err);
		});
	};

	module.zrange = function(key, start, stop, callback) {
		getSortedSetRange(key, start, stop, 1, false, callback);
	};

	module.zrevrange = function(key, start, stop, callback) {
		getSortedSetRange(key, start, stop, -1, false, callback);
	};

	module.zrangewithscores = function(key, start, stop, callback) {
		getSortedSetRange(key, start, stop, 1, true, callback);
	};

	module.zrevrangewithscores = function(key, start, stop, callback) {
		getSortedSetRange(key, start, stop, -1, true, callback);
	};

	function getSortedSetRange(key, start, stop, sort, withScores, callback) {
		if (!key) {
			return callback();
		}

		rangeQuery({k: key}, start, stop - start + 1, sort, withScores, callback);
	}

	module.zrangebyscore = function(key, start, count, min, max, callback) {
		getSortedSetRangeByScore(key, start, count, min, max, 1, false, callback);
	};

	module.zrevrangebyscore = function(key, start, count, max, min, callback) {
		getSortedSetRangeByScore(key, start, count, min, max, -1, false, callback);
	};

	module.zrangebyscorewithscores = function(key, start, count, min, max, callback) {
		getSortedSetRangeByScore(key, start, count, min, max, 1, true, callback);
	};

	module.zrevrangebyscorewithscores = function(key, start, count, max, min, callback) {
		getSortedSetRangeByScore(key, start, count, min, max, -1, true, callback);
	};

	function getSortedSetRangeByScore(key, start, count, min, max, sort, withScores, callback) {
		if (!key) {
			return callback();
		}

		if (parseInt(count, 10) === -1) {
			count = 0;
		}

		var scoreQuery = {};
		if (min !== '-inf') {
			scoreQuery.$gte = min;
		}
		if (max !== '+inf') {
			scoreQuery.$lte = max;
		}

		rangeQuery({k: key, s: scoreQuery}, start, count, sort, withScores, callback);
	}

	function rangeQuery(query, skip, limit, sort, withScores, callback) {
		var fields = {_id: 0, v: 1};
		if (withScores) {
			fields.s = 1;
		}
		db.collection('zset').find(query, {fields: fields})
			.sort({s: sort})
			.skip(skip)
			.limit(limit)
			.toArray(function(err, data) {
				if (err || !data) {
					return callback(err);
				}

				if (withScores) {
					data = data.map(function(item) {
						return {
							value: item.v,
							score: item.s
						};
					});
				} else {
					data = data.map(function(item) {
						return item.v;
					});
				}

				callback(null, data);
			});
	}

	module.zcount = function(key, min, max, callback) {
		if (!key) {
			return callback(null, 0);
		}

		var query = {k: key};
		if (min !== '-inf') {
			query.s = {$gte: min};
		}
		if (max !== '+inf') {
			query.s = query.score || {};
			query.s.$lte = max;
		}

		db.collection('zset').count(query, function(err, count) {
			callback(err, count ? count : 0);
		});
	};

	module.zcard = function(key, callback) {
		if (!key) {
			return callback(null, 0);
		}

		if (Array.isArray(key)) {
			return module.sortedSetsCard(key, callback);
		}

		db.collection('zset').count({k: key}, function(err, count) {
			count = parseInt(count, 10) || 0;
			callback(err, count);
		});
	};

	module.sortedSetsCard = function(keys, callback) {
		if (!Array.isArray(keys) || !keys.length) {
			return callback();
		}
		var pipeline = [
			{ $match : { k : { $in: keys } } } ,
			{ $group: { _id: {k: '$k'}, count: { $sum: 1 } } },
			{ $project: { _id: 1, count: '$count' } }
		];
		db.collection('zset').aggregate(pipeline, function(err, results) {
			if (err) {
				return callback(err);
			}

			if (!Array.isArray(results)) {
				results = [];
			}

			var map = {};
			results.forEach(function(item) {
				if (item && item._id.k) {
					map[item._id.k] = item.count;
				}
			});

			results = keys.map(function(key) {
				return map[key] || 0;
			});
			callback(null, results);
		});
	};

	module.zrank = function(key, value, callback) {
		getSortedSetRank('$lt', key, value, callback);
	};

	module.zrevrank = function(key, value, callback) {
		getSortedSetRank('$gt', key, value, callback);
	};

	function getSortedSetRank(q, key, value, callback) {
		if (!key) {
			return callback();
		}
		value = helpers.valueToString(value);
		module.zscore(key, value, function(err, score) {
			if (err) {
				return callback(err);
			}

			var query = {k: key, s: {}};
			query.s[q] = score;

			db.collection('zset').count(query, callback);
		});
	}

	module.zscore = function(key, value, callback) {
		if (!key) {
			return callback();
		}
		value = helpers.valueToString(value);
		db.collection('zset').findOne({k: key, v: value}, {fields:{_id: 0, s: 1}}, function(err, result) {
			callback(err, result ? result.s : null);
		});
	};

	module.sortedSetsScore = function(keys, value, callback) {
		if (!Array.isArray(keys) || !keys.length) {
			return callback();
		}
		value = helpers.valueToString(value);
		db.collection('objects').find({_key:{$in:keys}, value: value}, {_id:0, _key:1, score: 1}).toArray(function(err, result) {
			if (err) {
				return callback(err);
			}

			var map = helpers.toMap(result),
				returnData = [],
				item;

			for(var i=0; i<keys.length; ++i) {
				item = map[keys[i]];
				returnData.push(item ? item.score : null);
			}

			callback(null, returnData);
		});
	};

	module.sortedSetScores = function(key, values, callback) {
		if (!key) {
			return callback();
		}
		values = values.map(helpers.valueToString);
		db.collection('objects').find({_key: key, value: {$in: values}}, {_id: 0, value: 1, score: 1}).toArray(function(err, result) {
			if (err) {
				return callback(err);
			}

			var map = {};
			result.forEach(function(item) {
				map[item.value] = item.score;
			});

			var	returnData = new Array(values.length),
				score;

			for(var i=0; i<values.length; ++i) {
				score = map[values[i]];
				returnData[i] = score ? score : null;
			}

			callback(null, returnData);
		});
	};

	module.zismember = function(key, value, callback) {
		if (!key) {
			return callback(null, false);
		}
		value = helpers.valueToString(value);
		db.collection('zset').findOne({k: key, v: value}, {_id: 0, value: 1}, function(err, result) {
			callback(err, !!result);
		});
	};

	module.isSortedSetMembers = function(key, values, callback) {
		if (!key) {
			return callback();
		}
		values = values.map(helpers.valueToString);
		db.collection('objects').find({_key: key, value: {$in: values}}, {fields: {_id: 0, value: 1}}).toArray(function(err, results) {
			if (err) {
				return callback(err);
			}

			results = results.map(function(item) {
				return item.value;
			});

			values = values.map(function(value) {
				return results.indexOf(value) !== -1;
			});
			callback(null, values);
		});
	};

	module.isMemberOfSortedSets = function(keys, value, callback) {
		if (!Array.isArray(keys)) {
			return callback();
		}
		value = helpers.valueToString(value);
		db.collection('objects').find({_key: {$in: keys}, value: value}, {fields: {_id: 0, _key: 1, value: 1}}).toArray(function(err, results) {
			if (err) {
				return callback(err);
			}

			results = results.map(function(item) {
				return item._key;
			});

			results = keys.map(function(key) {
				return results.indexOf(key) !== -1;
			});
			callback(null, results);
		});
	};

	module.getSortedSetsMembers = function(keys, callback) {
		if (!Array.isArray(keys) || !keys.length) {
			return callback(null, []);
		}
		db.collection('objects').find({_key: {$in: keys}}, {_id: 0, _key: 1, value: 1}).toArray(function(err, data) {
			if (err) {
				return callback(err);
			}

			var sets = {};
			data.forEach(function(set) {
			 	sets[set._key] = sets[set._key] || [];
			 	sets[set._key].push(set.value);
			});

			var returnData = new Array(keys.length);
			for(var i=0; i<keys.length; ++i) {
			 	returnData[i] = sets[keys[i]] || [];
			}
			callback(null, returnData);
		});
	};

	module.getSortedSetUnion = function(sets, start, stop, callback) {
		getSortedSetUnion(sets, 1, start, stop, callback);
	};

	module.getSortedSetRevUnion = function(sets, start, stop, callback) {
		getSortedSetUnion(sets, -1, start, stop, callback);
	};

	function getSortedSetUnion(sets, sort, start, stop, callback) {
		if (!Array.isArray(sets) || !sets.length) {
			return callback();
		}
		var limit = stop - start + 1;
		if (limit <= 0) {
			limit = 0;
		}

		var pipeline = [
			{ $match: { _key: {$in: sets}} },
			{ $group: { _id: {value: '$value'}, totalScore: {$sum : "$score"}} },
			{ $sort: { totalScore: sort} }
		];

		if (start) {
			pipeline.push({ $skip: start });
		}

		if (limit > 0) {
			pipeline.push({ $limit: limit });
		}

		pipeline.push({	$project: { _id: 0, value: '$_id.value' }});

		db.collection('objects').aggregate(pipeline, function(err, data) {
			if (err || !data) {
				return callback(err);
			}

			data = data.map(function(item) {
				return item.value;
			});
			callback(null, data);
		});
	}

	module.sortedSetIncrBy = function(key, increment, value, callback) {
		callback = callback || helpers.noop;
		if (!key) {
			return callback();
		}
		var data = {};
		value = helpers.fieldToString(value);
		data.score = parseInt(increment, 10);

		db.collection('objects').findAndModify({_key: key, value: value}, {}, {$inc: data}, {new: true, upsert: true}, function(err, result) {
			callback(err, result && result.value ? result.value.score : null);
		});
	};

	module.getSortedSetRangeByLex = function(key, min, max, start, count, callback) {
		var query = {_key: key};
		if (min !== '-') {
			query.value = {$gte: min};
		}
		if (max !== '+') {
			query.value = query.value || {};
			query.value.$lte = max;
		}
		db.collection('objects').find(query, {_id: 0, value: 1})
			.sort({value: 1})
			.skip(start)
			.limit(count === -1 ? 0 : count)
			.toArray(function(err, data) {
				if (err) {
					return callback(err);
				}
				data = data.map(function(item) {
					return item && item.value;
				});
				callback(err, data);
		});
	};
};