
'use strict';

/* globals before, after, describe, it */

var mrdb = require('../index');
var async = require('async');
var assert = require('assert');


before(function(done) {
	mrdb.init({}, done);
});


require('./keys')(mrdb);
require('./hash')(mrdb);
require('./set')(mrdb);
require('./zset')(mrdb);
require('./list')(mrdb);


after(function(done) {
	done();
});





