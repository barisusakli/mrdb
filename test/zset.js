'use strict';

/* globals before, describe, it */

var async = require('async');
var assert = require('assert');


module.exports = function(mrdb) {

	describe('zadd/zismember/zscore/zcard', function() {
		it('should add element to sorted set', function(done) {
			mrdb.zadd('zset1', 100000, 'value1', function(err) {
				assert.ifError(err);
				mrdb.zismember('zset1', 'value1', function(err, sismember) {
					assert.ifError(err);
					assert.strictEqual(sismember, true);
					done();
				});
			});
		});

		it('should return the score of the element', function(done) {
			mrdb.zscore('zset1', 'value1', function(err, score) {
				assert.ifError(err);
				assert.strictEqual(score, 100000);
				done();
			});
		});

		it('should add elements to sorted set', function(done) {
			mrdb.zadd('zset2', [1, 2, 3], ['value1', 'value2', 'value3'], function(err) {
				assert.ifError(err);
				mrdb.zcard('zset2', function(err, count) {
					assert.ifError(err);
					assert.strictEqual(count, 3);
					done();
				});
			});
		});

	});

	describe('zrank', function() {
		before(function(done) {
			mrdb.zadd('zrankset', [1, 2, 3], ['value1', 'value2', 'value3'], done);
		});

		it('should return the correct rank of value1', function(done) {
			mrdb.zrank('zrankset', 'value1', function(err, rank) {
				assert.ifError(err);
				assert.strictEqual(rank, 0);
				done();
			});
		});

		it('should return the correct rank of value1', function(done) {
			mrdb.zrevrank('zrankset', 'value1', function(err, rank) {
				assert.ifError(err);
				assert.strictEqual(rank, 2);
				done();
			});
		});
	});





};
