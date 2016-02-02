'use strict';

var mrdb = {};

var async = require('async');
var mongoClient = require('mongodb').MongoClient;


mrdb.init = function(options, callback) {
	callback = callback || function() {};

	var usernamePassword = '';
	if (options.username && options.password) {
		usernamePassword = options.username + ':' + encodeURIComponent(options.password) + '@';
	}

	// Sensible defaults for Mongo, if not set
	options.host = options.host || '127.0.0.1';
	options.port = options.port || 27017;
	options.database = options.database || '0';
	options.poolSize = parseInt(options.poolSize, 10) || 10;

	var hosts = options.host.split(',');
	var ports = options.port.toString().split(',');
	var servers = [];

	for (var i = 0; i < hosts.length; i++) {
		servers.push(hosts[i] + ':' + ports[i]);
	}

	var connString = 'mongodb://' + usernamePassword + servers.join() + '/' + options.database;

	var connOptions = {
		server: {
			poolSize: options.poolSize
		}
	};

	mongoClient.connect(connString, connOptions, function(err, db) {
		if (err) {
			console.error('Could not connect to your Mongo database. Mongo returned the following error: ' + err.message);
			return callback(err);
		}



		require('./keys')(db, mrdb);
		require('./hash')(db, mrdb);
		require('./set')(db, mrdb);
		require('./zset')(db, mrdb);
		require('./list')(db, mrdb);

		if (options.username && options.password) {
			db.authenticate(options.username, options.password, function (err) {
				if (err) {
					console.error(err.stack);
					process.exit();
				}
				createIndices();
			});
		} else {
			console.warn('You have no mongo password setup!');
			createIndices();
		}

		function createIndices() {
			console.info('[database] Checking database indices.');
			async.parallel([
				async.apply(createIndex, 'set', {k: 1, v: -1}, {background: true, unique: true}),
				async.apply(createIndex, 'zset', {k: 1, s: -1}, {background: true}),
				async.apply(createIndex, 'zset', {k: 1, v: -1}, {background: true, unique: true}),

				async.apply(createIndex, 'hash', {expireAt: 1}, {expireAfterSeconds: 0, background: true}),
				async.apply(createIndex, 'zset', {expireAt: 1}, {expireAfterSeconds: 0, background: true}),
				async.apply(createIndex, 'set', {expireAt: 1}, {expireAfterSeconds: 0, background: true}),
				async.apply(createIndex, 'list', {expireAt: 1}, {expireAfterSeconds: 0, background: true})
			], function(err) {
				if (err) {
					console.error('Error creating index ' + err.message);
				}
				callback(err);
			});
		}

		function createIndex(collection, index, options, callback) {
			db.collection(collection).ensureIndex(index, options, callback);
		}

		mrdb.flushdb = function(callback) {
			callback = callback || function() {};
			db.dropDatabase(callback);
		};

	});
};


module.exports = mrdb;
