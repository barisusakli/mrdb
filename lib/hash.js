"use strict";


module.exports = function(db, module) {
	var helpers = require('./helpers');

	function toMap(data) {
		var map = {};
		for (var i = 0; i<data.length; ++i) {
			map[data[i]._id] = data[i];
			data[i]._id = undefined;
		}
		return map;
	}

	module.hmset = function(key, data, callback) {
		callback = callback || helpers.noop;
		if (!key) {
			return callback();
		}

		db.collection('hash').update({_id: key}, {$set: data}, {upsert: true, w: 1}, function(err) {
			callback(err);
		});
	};

	module.hset = function(key, field, value, callback) {
		callback = callback || helpers.noop;
		if (!field) {
			return callback();
		}
		var data = {};
		field = helpers.fieldToString(field);
		data[field] = value;
		module.hmset(key, data, callback);
	};

	module.hgetall = function(keys, callback) {
		if (!keys) {
			return callback();
		}
		if (Array.isArray(keys)) {
			if (!keys.length) {
				return callback(null, []);
			}
			db.collection('hash').find({_id: {$in: keys}}).toArray(function(err, data) {
				if (err) {
					return callback(err);
				}

				var map = toMap(data);
				var returnData = [];

				for (var i=0; i<keys.length; ++i) {
					returnData.push(map[keys[i]]);
				}

				callback(null, returnData);
			});
		} else {
			db.collection('hash').findOne({_id: keys}, {_id: 0}, callback);
		}
	};

	module.hget = function(key, field, callback) {
		if (!key) {
			return callback();
		}
		field = helpers.fieldToString(field);
		var _fields = {
			_id: 0
		};
		_fields[field] = 1;
		db.collection('hash').findOne({_id: key}, {fields: _fields}, function(err, item) {
			if (err || !item) {
				return callback(err, null);
			}

			callback(null, item.hasOwnProperty(field) ? item[field] : null);
		});
	};

	module.hmget = function(keys, fields, callback) {
		if (!keys) {
			return callback();
		}
		var _fields = {};

		for(var i=0; i<fields.length; ++i) {
			fields[i] = helpers.fieldToString(fields[i]);
			_fields[fields[i]] = 1;
		}

		if (Array.isArray(keys)) {
			db.collection('hash').find({_id: {$in: keys}}, {fields: _fields}).toArray(function(err, items) {
				if (err) {
					return callback(err);
				}

				if (items === null) {
					items = [];
				}

				var map = toMap(items);
				var returnData = [];
				var item;

				for (var i=0; i<keys.length; ++i) {
					item = map[keys[i]] || {};

					for (var k=0; k<fields.length; ++k) {
						if (item[fields[k]] === undefined) {
							item[fields[k]] = null;
						}
					}
					returnData.push(item);
				}

				callback(null, returnData);
			});
		} else {
			db.collection('hash').findOne({_id: keys}, {fields: _fields}, function(err, item) {
				if (err) {
					return callback(err);
				}
				item = item || {};
				var result = {};
				for(i=0; i<fields.length; ++i) {
					result[fields[i]] = item[fields[i]] !== undefined ? item[fields[i]] : null;
				}
				callback(null, result);
			});
		}
	};

	module.hkeys = function(key, callback) {
		module.getObject(key, function(err, data) {
			callback(err, data ? Object.keys(data) : []);
		});
	};

	module.hvals = function(key, callback) {
		module.getObject(key, function(err, data) {
			if(err) {
				return callback(err);
			}

			var values = [];
			for(var key in data) {
				if (data && data.hasOwnProperty(key)) {
					values.push(data[key]);
				}
			}
			callback(null, values);
		});
	};

	module.hexists = function(key, fields, callback) {
		if (!key) {
			return callback();
		}

		var data = {};

		if (Array.isArray(fields)) {
			fields.forEach(function(field) {
				field = helpers.fieldToString(field);
				data[field] = '';
			});

			db.collection('hash').findOne({_key: key}, {fields: data}, function(err, item) {
				if (err) {
					return callback(err);
				}
				var results = [];

				fields.forEach(function(field, index) {
					results[index] = !!item && item[field] !== undefined && item[field] !== null;
				});

				callback(null, results);
			});
		} else {
			fields = helpers.fieldToString(fields);
			data[fields] = '';
			db.collection('hash').findOne({_id: key}, {fields: data}, function(err, item) {
				callback(err, !!item && item[fields] !== undefined && item[fields] !== null);
			});
		}
	};

	module.hdel = function(key, fields, callback) {
		callback = callback || helpers.noop;

		if (!key) {
			return callback();
		}

		fields = Array.isArray(fields) ? fields : [fields];

		fields = fields.filter(Boolean);

		if (!fields.length) {
			return callback();
		}

		var data = {};
		fields.forEach(function(field) {
			field = helpers.fieldToString(field);
			data[field] = '';
		});

		db.collection('hash').update({_id: key}, {$unset : data}, function(err, res) {
			callback(err);
		});
	};

	module.hincrby = function(key, field, value, callback) {
		callback = callback || helpers.noop;
		if (!key) {
			return callback();
		}
		var data = {};
		field = helpers.fieldToString(field);
		data[field] = value;

		db.collection('hash').findAndModify({_id: key}, {}, {$inc: data}, {new: true, upsert: true}, function(err, result) {
			callback(err, result && result.value ? result.value[field] : null);
		});
	};

	module.hlen = function(key, callback) {
		module.hgetall(key, function(err, data) {
			if (err) {
				return callback(err);
			}

			callback(null, data ? Object.keys(data).length : 0);
		});
	};
};