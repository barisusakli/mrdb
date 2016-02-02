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
				existsIn('hash', {_id: key}, next);
			},
			function (next) {
				existsIn('zset', {k: key}, next);
			},
			function (next) {
				existsIn('set', {k: key}, next);
			},
			function (next) {
				existsIn('list', {_id: key}, next);
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

		async.parallel([
			function (next) {
				db.collection('hash').remove({_id: {$in: keys}}, next);
			},
			function (next) {
				db.collection('set').remove({k: {$in: keys}}, next);
			},
			function (next) {
				db.collection('zset').remove({k: {$in: keys}}, next);
			},
			function (next) {
				db.collection('list').remove({_id: {$in: keys}}, next);
			}
		], function(err) {
			callback(err);
		});
	};

	module.get = function(key, callback) {
		if (!key) {
			return callback();
		}
		module.getObjectField(key, 'value', callback);
	};

	module.set = function(key, value, callback) {
		callback = callback || helpers.noop;
		if (!key) {
			return callback();
		}
		var data = {value: value};
		module.setObject(key, data, callback);
	};

	module.increment = function(key, callback) {
		callback = callback || helpers.noop;
		if (!key) {
			return callback();
		}
		db.collection('hash').findAndModify({_id: key}, {}, {$inc: {value: 1}}, {new: true, upsert: true}, function(err, result) {
			callback(err, result && result.value ? result.value.value : null);
		});
	};

	module.rename = function(oldKey, newKey, callback) {
		callback = callback || helpers.noop;

		async.parallel([
			function (next) {
				db.collection('hash').rename({_id: oldKey}, {$set: {_id: newKey}}, next);
			},
			function (next) {
				db.collection('set').rename({k: oldKey}, {$set: {k: newKey}}, {multi: true}, next);
			},
			function (next) {
				db.collection('zset').rename({k: oldKey}, {$set: {k: newKey}}, {multi: true}, next);
			},
			function (next) {
				db.collection('list').rename({_id: oldKey}, {$set: {_id: newKey}}, next);
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
				expire('hash', {_id: key}, date, next);
			},
			function (next) {
				expire('zset', {k: key}, date, next);
			},
			function (next) {
				expire('set', {k: key}, date, next);
			},
			function (next) {
				expire('list', {_id: key}, date, next);
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