var	mongoose = require('mongoose'),
	db = exports;

exports.user_item_value_schema = new mongoose.Schema({ user: String, item: String, value: Number });
exports.insert = mongodb_insert;
exports.bulk_load = mongodb_bulk_load;
exports.dropDatabase = mongodb_dropDatabase;
exports.import_by_stream = mongodb_import_by_stream;

function mongodb_import_by_stream(server, collection, output_stream, input_stream, field_split_by, callback) {
	var moment = require('moment');
	var readline = require('readline');
	var rl = readline.createInterface({
		input: input_stream,
		output: output_stream,
		terminal: false
	});
	var records = [];
	var cnt = 0;
	var batch_mode_cnt = 8192;
	rl.on('line', function(line){
		var field = line.split(field_split_by);
		if (field.length == 3)
			records.push( { user: field[0] , item: field[1], value : parseFloat(field[2]) } );
		if (records.length > batch_mode_cnt) {
			rl.pause();
			var data = records;
			records = [];
			mongodb_bulk_load(server, collection, data, function(){
				console.log('\t\t['+moment().format()+'] raw import, record count: '+data.length);
				rl.resume();
			});
		}
		cnt++;
	}).on('close', function() {
		if (records.length) {
			var data = records;
			records = [];
			mongodb_bulk_load(server, collection, data, function(){
				console.log('\t\t['+moment().format()+'] raw import, record count: '+data.length);
				console.log('[INFO] raw import, total record count: '+cnt + ' @ ' + moment().format());
				if(callback)
					callback("["+moment().format() +'] raw count:\t'+cnt);
			});
		} else if(callback)
			callback("["+moment().format() +'] raw count:\t'+cnt);
	});
}

function mongodb_insert(server, collection, user, item, value, callback) {
	//console.log( 'Connect to [' + 'mongodb://'+server + ']');
	var conn = mongoose.createConnection('mongodb://'+server);
	conn.on('error', console.error.bind(console, 'connection error:'));
	conn.once('open', function() {
		var model = conn.model( 'rec', exports.user_item_value_schema, collection);

		var rec = new model;
		rec.user = user;
		rec.item = item;
		rec.value = value;
		rec.save( function (err) {
			if (err)
				console.log('save error: '+err);
			conn.close();
			if (callback)
				callback();
		});
	});
}

function mongodb_bulk_load(server, collection, record_array, callback) {
	//console.log( 'Connect to [' + 'mongodb://'+server + ']');
	var conn = mongoose.createConnection('mongodb://'+server);
	conn.on('error', console.error.bind(console, 'connection error:'));
	conn.once('open', function () {
		//console.log('open');
		var model = conn.model( 'rec', exports.user_item_value_schema, collection);
		model.collection.insert(record_array, {w:1}, function (err) {
			if (err)
				console.log(err);
			conn.close();
			if (callback)
				callback();
		});
	});
}

function mongodb_dropDatabase(server, callback) {
	var conn = mongoose.createConnection('mongodb://'+server);
	conn.on('error', console.error.bind(console, 'connection error:'));
	conn.once('open', function () {
		conn.db.dropDatabase();
		conn.close();
		if (callback)
			callback();
	});
}

