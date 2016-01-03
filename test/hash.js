'use strict';

/* globals before, describe, it */

var async = require('async');
var assert = require('assert');


module.exports = function(mrdb) {

	describe('hdel/hexists', function() {
		before(function(done) {
			mrdb.hset('hash1', 'field1', 'bar', done);
		});

		it('should delete a hash field', function(done) {
			mrdb.hdel('hash1', 'field1', function(err) {
				assert.ifError(err);
				mrdb.hexists('hash1', 'field1', function(err, exists) {
					assert.ifError(err);
					assert.strictEqual(exists, false);
					done();
				});
			});
		});
	});

	describe('hgetall/hmset', function() {
		var obj = {
			firstname: 'baris',
			lastname: 'usakli',
			age: 99
		};
		before(function(done) {
			mrdb.hmset('user:1', obj, done);
		});

		it('should get all the fields of a hash', function(done) {
			mrdb.hgetall('user:1', function(err, data) {
				assert.ifError(err);
				assert.deepStrictEqual(data, obj);
				done();
			});
		});
	});

	describe('hmget', function() {
		var obj = {
			firstname: 'john',
			lastname: 'doe',
			age: 23
		};

		before(function(done) {
			mrdb.hmset('user:2', obj, done);
		});

		it('should get multiple fields of a hash', function(done) {
			mrdb.hmget('user:2', ['firstname', 'age'], function(err, data) {
				assert.ifError(err);
				assert.strictEqual(obj.firstname, data.firstname);
				assert.strictEqual(obj.age, data.age);
				done();
			});
		});
	});

	describe('hlen', function() {
		var obj = {
			field1: 'john',
			field2: 'doe',
			field3: 23,
			field4: 'foo'
		};

		before(function(done) {
			mrdb.hmset('user:3', obj, done);
		});

		it('should return the number of fields in a hash', function(done) {
			mrdb.hlen('user:3', function(err, fieldCount) {
				assert.ifError(err);
				assert.strictEqual(fieldCount, 4);
				done();
			});
		});
	});

};