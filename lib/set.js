"use strict";

module.exports = function(db, module) {
	var async = require('async');
	var helpers = require('./helpers');


	module.sadd = function(key, value, callback) {
		callback = callback || helpers.noop;
		if (Array.isArray(key)) {
			return module.setsAdd(key, value, callback);
		}
		if (!Array.isArray(value)) {
			value = [value];
		}

		value = value.map(helpers.valueToString);

		var bulk = db.collection('set').initializeUnorderedBulkOp();

		value.forEach(function(value) {
			bulk.find({k: key, v: value}).upsert().updateOne({$set: {v: value}});
		});

		bulk.execute(function(err) {
			callback(err);
		});
	};

	module.setsAdd = function(keys, value, callback) {
		callback = callback || helpers.noop;

		if (!Array.isArray(keys) || !keys.length) {
			return callback();
		}

		if (!Array.isArray(value)) {
			value = [value];
		}

		value = value.map(helpers.valueToString);

		var bulk = db.collection('set').initializeUnorderedBulkOp();

		keys.forEach(function(key) {
			value.forEach(function(value) {
				bulk.find({k: key, v: value}).upsert().updateOne({$set: {v: value}});
			});
		});

		bulk.execute(function(err) {
			callback(err);
		});
	};

	module.sremove = function(key, value, callback) {
		callback = callback || helpers.noop;

		if (Array.isArray(key)) {
			return module.setsRemove(key, value, callback);
		}

		if (!Array.isArray(value)) {
			value = [value];
		}

		value = value.map(helpers.valueToString);

		db.collection('set').remove({k: key, v: {$in: value}}, function(err) {
			callback(err);
		});
	};

	module.setsRemove = function(keys, value, callback) {
		callback = callback || helpers.noop;
		if (!Array.isArray(keys) || !keys.length) {
			return callback();
		}
		value = helpers.valueToString(value);

		db.collection('set').remove({k: {$in: keys}, v: value}, function(err) {
			callback(err);
		});
	};

	module.sismember = function(key, value, callback) {
		if (!key) {
			return callback(null, false);
		}
		if (Array.isArray(key)) {
			return module.isMemberOfSets(key, value, callback);
		}
		if (Array.isArray(value)) {
			return module.isSetMembers(key, value, callback);
		}
		value = helpers.valueToString(value);

		db.collection('set').findOne({k: key, v: value}, {_id: 0}, function(err, item) {
			callback(err, item !== null && item !== undefined);
		});
	};

	module.isSetMembers = function(key, values, callback) {
		if (!key || !Array.isArray(values) || !values.length) {
			return callback(null, []);
		}

		values = values.map(helpers.valueToString);

		db.collection('set').find({k: key, v: {$in: values}}, {fields: {_id: 0, v: 1}}).toArray(function(err, results) {
			if (err) {
				return callback(err);
			}

			results = results.map(function(item) {
				return item.v;
			});

			values = values.map(function(value) {
				return results.indexOf(value) !== -1;
			});
			callback(null, values);
		});
	};

	module.isMemberOfSets = function(sets, value, callback) {
		if (!Array.isArray(sets) || !sets.length) {
			return callback(null, []);
		}
		value = helpers.valueToString(value);

		db.collection('set').find({k: {$in: sets}, v: value}, {_id: 0, v: 0}).toArray(function(err, result) {
			if (err) {
				return callback(err);
			}

			result = result.map(function(item) {
				return item.k;
			});

			result = sets.map(function(set) {
				return result.indexOf(set) !== -1;
			});

			callback(null, result);
		});
	};

	module.smembers = function(key, callback) {
		if (!key) {
			return callback(null, []);
		}
		if (Array.isArray(key)) {
			return module.getSetsMembers(key, callback);
		}

		db.collection('set').find({k: key}, {_id: 0, k: 0}).toArray(function(err, data) {
			if (err) {
				return callback(err);
			}

			data = data.map(function(item) {
				return item && item.v;
			});

			callback(null, data);
		});
	};

	module.getSetsMembers = function(keys, callback) {
		if (!Array.isArray(keys) || !keys.length) {
			return callback(null, []);
		}

		db.collection('set').find({k: {$in: keys}}, {_id: 0, k: 1, v: 1}).toArray(function(err, data) {
			if (err) {
				return callback(err);
			}

			var sets = {};
			data.forEach(function(set) {
				sets[set.k] = sets[set.k] || [];
				sets[set.k].push(set.v);
			});

			var returnData = new Array(keys.length);
			for(var i=0; i<keys.length; ++i) {
				returnData[i] = sets[keys[i]] || [];
			}
			callback(null, returnData);
		});
	};

	module.scard = function(key, callback) {
		if (!key) {
			return callback(null, 0);
		}
		if (Array.isArray(key)) {
			async.map(key, function(key, next) {
				db.collection('set').count({k: key}, next);
			}, callback);
		} else {
			db.collection('set').count({k: key}, callback);
		}
	};

};