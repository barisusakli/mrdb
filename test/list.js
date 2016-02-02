'use strict';

/* globals before, describe, it */

var async = require('async');
var assert = require('assert');


module.exports = function(mrdb) {

	describe('lpush', function() {
		before(function(done) {
			mrdb.delete('list1', done);
		});

		it('should add element to the end of list', function(done) {
			mrdb.lpush('list1', 'value1', function(err) {
				assert.ifError(err);
				mrdb.lrange('list1', 0, -1, function(err, list) {
					assert.ifError(err);
					assert.strictEqual(list[0], 'value1');
					done();
				});
			});
		});
	});



};
