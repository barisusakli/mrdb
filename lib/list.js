"use strict";

module.exports = function(db, module) {
	var helpers = require('./helpers');

	module.lpush = function(key, value, callback) {
		callback = callback || helpers.noop;

		if (!key) {
			return callback();
		}

		if (!Array.isArray(value)) {
			value = [value];
		}

		value = value.map(helpers.valueToString);

		db.collection('list').findOne({_id: key}, {array: 0}, function(err, item) {
			if (err) {
				return callback(err);
			}

			if (item && item._id) {
				var bulk = db.collection('list').initializeOrderedBulkOp();

				for(var i=0; i<value.length; ++i) {
					bulk.find({_id: key}).upsert().updateOne({$push: {array: {$each: [value[i]], $position: 0}}});
				}

				bulk.execute(function(err) {
					callback(err);
				});
			} else {
				module.rpush(key, value, callback);
			}
		});
	};

	module.rpush = function(key, value, callback) {
		callback = callback || helpers.noop;
		if (!key) {
			return callback();
		}

		if (!Array.isArray(value)) {
			value = [value];
		}

		value = value.map(helpers.valueToString);
		db.collection('list').update({_id: key }, {$push: {array: {$each: value}}}, {upsert: true, w: 1}, function(err) {
			callback(err);
		});
	};

	module.rpop = function(key, callback) {
		callback = callback || helpers.noop;
		if (!key) {
			return callback();
		}
		module.lrange(key, -1, -1, function(err, value) {
			if (err) {
				return callback(err);
			}

			db.collection('list').update({_key: key }, { $pop: { array: 1 } }, function(err) {
				callback(err, (value && value.length) ? value[0] : null);
			});
		});
	};

	module.ltrim = function(key, start, stop, callback) {
		callback = callback || helpers.noop;
		if (!key) {
			return callback();
		}
		module.lrange(key, start, stop, function(err, value) {
			if (err) {
				return callback(err);
			}

			db.collection('objects').update({_id: key}, {$set: {array: value}}, function(err) {
				callback(err);
			});
		});
	};

	module.lrange = function(key, start, stop, callback) {
		if (!key) {
			return callback();
		}

		db.collection('list').findOne({_id: key}, {array: 1}, function(err, data) {
			if (err || !(data && data.array)) {
				return callback(err, []);
			}

			if (stop === -1) {
				data.array = data.array.slice(start);
			} else {
				data.array = data.array.slice(start, stop + 1);
			}
			callback(null, data.array);
		});
	};
};