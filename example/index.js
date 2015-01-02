var async = require('async');
var sleep = require('sleep');
var db = require('../lib/mongodb_import');
var fs = require('fs');
var readline = require('readline');
var calc = require('../lib/mongodb_calc');

var raw_input = null;
var raw_split_by = '|';
var target_mongodb = 'localhost/test';
var raw_mongodb_collection = 'raw';
var recommended_mongodb_collection = 'recommendation';

var meta_user_item = 'user_item';
var meta_uniq_user = 'user';
var meta_uniq_item = 'item';
//var meta_user_item_list = 'user_item_list';	// no use
var meta_co_matrix_prepare = 'co_matrix_prepare';
var meta_user_item_filter = 'user_item_filter';
var meta_co_matrix = 'co_matrix';
var meta_matrix_multiplication_usage = 'rec_matrix_mul_usage';

var myArgs = process.argv.slice(2);
for (var i=0, len=myArgs.length ; i<len ; ++i) {
	switch(myArgs[i]) {
		case '-file':
			if (i + 1<len)
				raw_input = myArgs[i+1];
			++i;
			break;
		case '-split':
			if (i + 1<len)
				raw_split_by = myArgs[i+1];
			++i;
			break;
		case '-mongodb-db':
			if (i + 1<len)
				target_mongodb = myArgs[i+1];
			++i;
			break;
		case '-raw-mongodb-collection':
			if (i + 1<len)
				raw_mongodb_collection = myArgs[i+1];
			++i;
			break;
		case '-recommended-mongodb-collection':
			if (i + 1<len)
				recommended_mongodb_collection = myArgs[i+1];
			++i;
			break;
	}
}

console.log('[INFO] INPUT: '+ (raw_input|| "stdin") );
console.log('[INFO] INPUT FILED BY: '+raw_split_by);
console.log('[INFO] TARGET MONGODB DB: '+target_mongodb);
console.log('[INFO] RAW MONGODB COLLECTION: '+raw_mongodb_collection);
console.log('[INFO] RECOMMENDED MONGODB COLLECTION: '+recommended_mongodb_collection);

async.series([
	// Step 0: reset data
	function(callback) {
		db.dropDatabase(target_mongodb, function() {
			callback(null, '[DONE] Step 0: Reset database');
		});
	},

	// Step 1: import data
	function(callback) {
		db.import_by_stream(target_mongodb, raw_mongodb_collection, process.stdout, raw_input ? fs.createReadStream(raw_input) : process.stdin , raw_split_by, function(data) {
			callback(null, '[DONE] Step 1: Import data count:\t\t\t'+ data);
		});
	},

	// Step 2: build user-item based record
	function(callback) {
		var moment = require('moment');
		console.log ('[INFO] building user-item pair @ ' + moment().format());
		calc.build_user_item_pair_with_sum_value(target_mongodb, raw_mongodb_collection, 'user', 'item', 'value', meta_user_item, function(data) {
			callback(null, '[DONE] Step 2: build user-item based record:\t\t' +data);
		});
	},

	// Step 3: find uniq user (fast version)
	function(callback) {
		var moment = require('moment');
		console.log ('[INFO] building uniq user @ ' + moment().format());
		calc.find_uniq_target_via_user_item_pair(target_mongodb, meta_user_item, 'user', meta_uniq_user, function(data) {
			callback(null, '[DONE] Step 3: find uniq user:\t\t\t\t'+data);
		});
	},

	// Step 4: find uniq item (fast version)
	function(callback) {
		var moment = require('moment');
		console.log ('[INFO] building uniq item @ ' + moment().format());
		calc.find_uniq_target_via_user_item_pair(target_mongodb, meta_user_item, 'item', meta_uniq_item, function(data) {
			callback(null, '[DONE] Step 4: find uniq item:\t\t\t\t'+data);
		});
	},

	//// Step 5: build user-item list (fast version)
	//function(callback) {
	//	var moment = require('moment');
	//	console.log ('[INFO] building user-item list @ ' + moment().format());
	//	calc.build_user_item_list_via_user_item_pair(target_mongodb, meta_user_item, 'user', 'item', meta_user_item_list, function(data) {
	//		callback(null, '[DONE] Step 5: build user-item list:\t\t\t'+data);
	//	});
	//},

	//// Step 6: build Item-based Co-Occurrence Matrix
	//function(callback) {
	//	var moment = require('moment');
	//	console.log ('[INFO] building co-occurrence matrix @ ' + moment().format());
	//	calc.build_item_based_co_occurrence_matrix(target_mongodb, meta_user_item_list, 'item', meta_co_matrix, function(data) {
	//		callback(null, '[DONE] Step 6: Item-based Co-Occurrence Matrix:\t\t' + data);
	//	});
	//},

	// Step 5 & 6: build Item-based Co-Occurrence Matrix
	function(callback) {
		var moment = require('moment');
		console.log ('[INFO] building co-occurrence matrix @ ' + moment().format());
		calc.build_item_based_co_occurrence_matrix(target_mongodb, meta_user_item, meta_uniq_user, meta_user_item_filter, meta_co_matrix_prepare, meta_co_matrix, function(data) {
			callback(null, '[DONE] Step 5&6: Item-based Co-Occurrence Matrix:\t' + data);
		});
	},



	// Step 7: build recommendation
	function(callback) {
		var moment = require('moment');
		console.log ('[INFO] building recommendation @ ' + moment().format());
		calc.build_recommendation(target_mongodb, meta_uniq_user, meta_uniq_item, meta_user_item, meta_co_matrix, meta_matrix_multiplication_usage, recommended_mongodb_collection, function(data) {
			callback(null, '[DONE] Step 7: build user-item-based recommendation:\t' + data);
		});
	},
], function(err, result){
	if (err)
		console.log(err);
	console.log("\n\n");
	for (var i=0, cnt=result.length ; i<cnt ; ++i)
		console.log(result[i]);
});
