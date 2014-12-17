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
var meta_user_item_list = 'user_item_list';
var meta_co_matrix_init = 'co_matrix_init';
var meta_co_matrix = 'co_matrix';
var meta_user_prefer = 'user_prefer';

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
//process.argv.forEach(function (val, index, array) {
	//console.log(index + ': ' + val);
//});

console.log('[INFO] INPUT: '+ (raw_input|| "stdin") );
console.log('[INFO] INPUT FILED BY: '+raw_split_by);
console.log('[INFO] TARGET MONGODB DB: '+target_mongodb);
console.log('[INFO] RAW MONGODB COLLECTION: '+raw_mongodb_collection);
console.log('[INFO] RECOMMENDED MONGODB COLLECTION: '+recommended_mongodb_collection);

async.series([

	// Step 0: import data
	function(callback) {
		import_data(function(data){
			callback(null, data);
		});
	},
	// Step 1: build user-item based record
	function(callback) {
		calc.build_user_item_pair_with_sum_value(target_mongodb, raw_mongodb_collection, 'user', 'item', 'value', meta_user_item, function(data) {
			callback(null, data);
		});
	},

	// Step 2: find uniq user (fast version)
	function(callback) {
		calc.find_uniq_target_via_user_item_pair(target_mongodb, meta_user_item, 'user', meta_uniq_user, function(data) {
			callback(null, data);
		});
	},
	//// Step 2: find uniq user
	//function(callback) {
	//	calc.find_uniq_target(target_mongodb, raw_mongodb_collection, 'user', meta_uniq_user, function(data) {
	//		callback(null, data);
	//	});
	//},

	// Step 3: find uniq item (fast version)
	function(callback) {
		calc.find_uniq_target_via_user_item_pair(target_mongodb, meta_user_item, 'item', meta_uniq_item, function(data) {
			callback(null, data);
		});
	},

	//// Step 3: find uniq item
	//function(callback) {
	//	calc.find_uniq_target(target_mongodb, raw_mongodb_collection, 'item', meta_uniq_item, function(data) {
	//		callback(null, data);
	//	});
	//},

	// Step 4: build user-item list (fast version)
	function(callback) {
		calc.build_user_item_list_via_user_item_pair(target_mongodb, meta_user_item, 'user', 'item', meta_user_item_list, function(data) {
			callback(null, data);
		});
	},

	// Step 4: build user-item list
	//function(callback) {
	//	calc.build_user_item_list(target_mongodb, raw_mongodb_collection, 'user', 'item', meta_user_item_list, function(data) {
	//		callback(null, data);
	//	});
	//},

	// Step 5: Item-based Co-Occurrence Matrix - init
	function(callback) {
		calc.build_item_based_co_occurrence_matrix_prepare(target_mongodb, meta_user_item_list, 'item', meta_co_matrix_init, function(data) {
			callback(null, data);
		});
	},

	// Step 6: Item-based Co-Occurrence Matrix - done
	function(callback) {
		calc.build_item_based_co_occurrence_matrix(target_mongodb, meta_co_matrix_init, 'key', 'value', meta_co_matrix, function(data) {
			callback(null, data);
		});
	},

	// Step 7: build user prefer (fast version)
	function(callback) {
		calc.build_user_prefer_via_user_item_pair(target_mongodb, meta_user_item, meta_uniq_item, meta_co_matrix, meta_user_prefer, function(data) {
			callback(null, data);
		});
	},
	// Step 7: build user prefer
	//function(callback) {
	//	calc.build_user_prefer(target_mongodb, raw_mongodb_collection, meta_uniq_item, meta_co_matrix, meta_user_prefer, function(data) {
	//		callback(null, data);
	//	});
	//},

	// Step 8: build recommendation
	function(callback) {
		calc.build_recommendation(target_mongodb, meta_user_prefer, 'user', 'item', 'value', recommended_mongodb_collection, function(data) {
			callback(null, data);
		});
	},
	// Step #: clear 
	function(callback) {
		callback(null, 0);
	},
// */
], function(err, result){
	if (err)
		console.log(err);
	console.log( 'DONE' );
	for (var i=0, cnt=result.length ; i<cnt ; ++i)
		console.log(result[i]);
});


//
// import data
//
//db.insert(target_mongodb, raw_mongodb_collection, 'user','item', 99);
//db.bulk_load(target_mongodb, raw_mongodb_collection, [
//	{ user : 'u1', item : 'i1', value: 0 },
//	{ user : 'u2', item : 'i2', value: 1 }
//]);
function import_data(callback) {
	var input_from = process.stdin;
	if (raw_input) 
		input_from = fs.createReadStream(raw_input);
	
	var rl = readline.createInterface({
		input: input_from,
		output: process.stdout,
		terminal: false
	});
	var records = [];
	rl.on('line', function(line){
		//console.log(line)
		var field = line.split(raw_split_by);
		if (field.length == 3)
			records.push( { user: field[0] , item: field[1], value : parseFloat(field[2]) } );
	}).on('close', function() {
		db.bulk_load(target_mongodb, raw_mongodb_collection, records, function(){
			console.log('[INFO] raw import, record count: '+records.length);
			if(callback)
				callback(records.length);
		});
	});
}
