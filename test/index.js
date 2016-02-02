
'use strict';

/* globals before, after, describe, it */

var mrdb = require('../index');
var async = require('async');
var assert = require('assert');


before(function(done) {
	mrdb.init({}, done);
});


require('./hash')(mrdb);
require('./set')(mrdb);


after(function(done) {
	done();
});




