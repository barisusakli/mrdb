"use strict";

var async = require('async');
var helpers = {};

helpers.some = function(tasks, callback) {
	async.some(tasks, function(task, next) {
		task(function(err, result) {
			next(!err && result);
		});
	}, function(result) {
		callback(null, result);
	});
};

helpers.fieldToString = function(field) {
	if(field === null || field === undefined) {
		return field;
	}

	if(typeof field !== 'string') {
		field = field.toString();
	}
	// if there is a '.' in the field name it inserts subdocument in mongo, replace '.'s with \uff0E
	field = field.replace(/\./g, '\uff0E');
	return field;
};

helpers.valueToString = function(value) {
	if(value === null || value === undefined) {
		return value;
	}

	return value.toString();
};

helpers.noop = function() {};

module.exports = helpers;