"use strict";


var async = require('async');

module.exports = function(db, module) {

	var helpers = require('./helpers');

	function existsIn(collection, query, callback) {
		db.collection(collection).findOne(query, function(err, item) {
			callback(err, item !== undefined && item !== null);
		});
	}

	module.exists = function(key, callback) {
		if (!key) {
			return callback();
		}

		helpers.some([
			function (next) {
				existsIn('hash', {k: key}, next);
			},
			function (next) {
				existsIn('zset', {k: key}, next);
			},
			function (next) {
				existsIn('set', {k: key}, next);
			},
			function (next) {
				existsIn('list', {k: key}, next);
			}
		], callback);
	};

	module.delete = function(keys, callback) {
		callback = callback || helpers.noop;
		if (!keys) {
			return callback();
		}
		if (!Array.isArray(keys)) {
			keys = [keys];
		}

		if (!keys.length) {
			return callback();
		}

		async.each(['hash', 'set', 'zset', 'list'], function(collection, next) {
			db.collection(collection).remove({k: {$in: keys}}, next);
		}, callback);
	};

	module.get = function(key, callback) {
		if (!key) {
			return callback();
		}
		module.hget(key, 'value', callback);
	};

	module.set = function(key, value, callback) {
		callback = callback || helpers.noop;
		if (!key) {
			return callback();
		}
		var data = {value: value};
		module.hmset(key, data, callback);
	};

	module.increment = function(key, callback) {
		callback = callback || helpers.noop;
		if (!key) {
			return callback();
		}
		db.collection('hash').findAndModify({k: key}, {}, {$inc: {value: 1}}, {new: true, upsert: true}, function(err, result) {
			callback(err, result && result.value ? result.value.value : null);
		});
	};

	module.rename = function(oldKey, newKey, callback) {
		callback = callback || helpers.noop;

		async.parallel([
			function (next) {
				db.collection('hash').update({k: oldKey}, {$set: {k: newKey}}, next);
			},
			function (next) {
				db.collection('set').update({k: oldKey}, {$set: {k: newKey}}, {multi: true}, next);
			},
			function (next) {
				db.collection('zset').update({k: oldKey}, {$set: {k: newKey}}, {multi: true}, next);
			},
			function (next) {
				db.collection('list').update({k: oldKey}, {$set: {k: newKey}}, next);
			}
		], function(err) {
			callback(err);
		});
	};

	module.expire = function(key, seconds, callback) {
		module.expireAt(key, Math.round(Date.now() / 1000) + seconds, callback);
	};

	module.expireAt = function(key, timestamp, callback) {
		module.pexpireAt(key, timestamp * 1000, callback);
	};

	module.pexpire = function(key, ms, callback) {
		module.pexpireAt(key, Date.now() + parseInt(ms, 10), callback);
	};

	module.pexpireAt = function(key, timestamp, callback) {
		var date = new Date(timestamp);
		async.parallel([
			function (next) {
				expire('hash', {k: key}, date, next);
			},
			function (next) {
				expire('zset', {k: key}, date, next);
			},
			function (next) {
				expire('set', {k: key}, date, next);
			},
			function (next) {
				expire('list', {k: key}, date, next);
			},
		], function(err) {
			callback(err);
		});
	};

	function expire(collection, query, value, callback) {
		db.collection(collection).update(query, {$set: {expireAt: value}}, {w: 1}, function(err) {
			callback(err);
		});
	}
};