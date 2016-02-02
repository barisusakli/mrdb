'use strict';

/* globals before, describe, it */

var async = require('async');
var assert = require('assert');


module.exports = function(mrdb) {

	describe('sadd/sismember', function() {
		it('should add element to set', function(done) {
			mrdb.sadd('set1', 'value1', function(err) {
				assert.ifError(err);
				mrdb.sismember('set1', 'value1', function(err, sismember) {
					assert.ifError(err);
					assert.strictEqual(sismember, true);
					done();
				});
			});
		});

		it('should add elements to set', function(done) {
			mrdb.sadd('set1', ['value1', 'value2', 'value3'], function(err) {
				assert.ifError(err);
				mrdb.sismember('set1', ['value2', 'value3', 'nothere'], function(err, sismember) {
					assert.ifError(err);
					assert.strictEqual(sismember[0], true);
					assert.strictEqual(sismember[1], true);
					assert.strictEqual(sismember[2], false);
					done();
				});
			});
		});

		it('should add elements to sets', function(done) {
			mrdb.sadd(['set1', 'set2'], 'value4', function(err) {
				assert.ifError(err);
				async.parallel({
					set1: function(next) {
						mrdb.sismember('set1', 'value4', next);
					},
					set2: function(next) {
						mrdb.sismember('set2', 'value4', next);
					}
				}, function(err, results) {
					assert.ifError(err);
					assert.strictEqual(results.set1, true);
					assert.strictEqual(results.set2, true);
					done();
				});
			});
		});

		it('should return false if not member', function(done) {
			mrdb.sismember('set1', 'notamember', function(err, sismember) {
				assert.ifError(err);
				assert.strictEqual(sismember, false);
				done();
			});
		});

		it('should return false if set does not exist', function(done) {
			mrdb.sismember('doesntexist', 'value1', function(err, sismember) {
				assert.ifError(err);
				assert.strictEqual(sismember, false);
				done();
			});
		});

		it('should check if value exists in multiple sets', function(done) {
			mrdb.sismember(['set1', 'set2'], 'value1', function(err, sismember) {
				assert.ifError(err);
				assert.strictEqual(sismember[0], true);
				assert.strictEqual(sismember[1], false);
				done();
			});
		});
	});

	describe('sremove', function() {
		before(function(done) {
			mrdb.sadd('sremoveset', ['1', '2', '3'], done);
		});

		it('should remove element from set', function(done) {
			mrdb.sremove('sremoveset', '2', function(err) {
				assert.ifError(err);
				mrdb.sismember('sremoveset', '2', function(err, sismember) {
					assert.ifError(err);
					assert.strictEqual(sismember, false);
					done();
				});
			});
		});

		it('should remove elements from set', function(done) {
			mrdb.sremove('sremoveset', ['3', '1'], function(err) {
				assert.ifError(err);
				mrdb.sismember('sremoveset', '1', function(err, sismember) {
					assert.ifError(err);
					assert.strictEqual(sismember, false);
					done();
				});
			});
		});
	});

	describe('scard', function() {
		before(function(done) {
			mrdb.sadd('scardset', ['1', '2', '3'], done);
		});

		it('should return the number of elements in set', function(done) {
			mrdb.scard('scardset', function(err, count) {
				assert.ifError(err);
				assert.strictEqual(count, 3);
				done();
			});
		});

		it('should return 0 if set does not exist', function(done) {
			mrdb.scard('doesntexist', function(err, count) {
				assert.ifError(err);
				assert.strictEqual(count, 0);
				done();
			});
		});

		it('should count both sets', function(done) {
			mrdb.scard(['scardset', 'doesntexist'], function(err, counts) {
				assert.ifError(err);
				assert.strictEqual(counts[0], 3);
				assert.strictEqual(counts[1], 0);
				done();
			});
		});
	});


	describe('smembers', function() {
		before(function(done) {
			async.parallel([
				async.apply(mrdb.sadd, 'smembersset1', ['1', '2', '3', '4', '5', '6']),
				async.apply(mrdb.sadd, 'smembersset2', ['7', '8', '9', '10', '11'])
			], done);
		});

		it('should return the elements in set', function(done) {
			mrdb.smembers('smembersset1', function(err, members) {
				assert.ifError(err);
				assert.strictEqual(members.length, 6);
				done();
			});
		});

		it('should return the elements in sets', function(done) {
			mrdb.smembers(['smembersset1', 'smembersset2'], function(err, members) {
				assert.ifError(err);
				assert.strictEqual(members[0].length, 6);
				assert.strictEqual(members[1].length, 5);
				done();
			});
		});
	});



};
