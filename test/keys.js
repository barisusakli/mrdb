'use strict';

/* globals before, describe, it */

var async = require('async');
var assert = require('assert');


module.exports = function(mrdb) {

	describe('keys', function() {
		it('should set a key', function(done) {
			mrdb.set('key1', 'key1value', function(err) {
				assert.ifError(err);
				mrdb.get('key1', function(err, value) {
					assert.ifError(err);
					assert.strictEqual(value, 'key1value');
					done();
				});
			});
		});

		describe('rename', function() {
			before(function(done) {
				async.parallel([
					function (next) {
						mrdb.set('oldName', 'value', next);
					},
					function (next) {
						mrdb.delete('newName', next);
					}
				], done);
			});

			it('should rename a key', function(done) {
				mrdb.rename('oldName', 'newName', function(err) {
					mrdb.exists('oldName', function(err, exists) {
						assert.ifError(err);
						assert.strictEqual(exists, false);
						done();
					});
				});
			});
		});
	});


};