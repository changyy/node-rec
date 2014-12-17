var	mongoose = require('mongoose'),
	db = exports;

exports.user_item_value_schema = new mongoose.Schema({ user: String, item: String, value: Number });
exports.insert = mongodb_insert;
exports.bulk_load = mongodb_bulk_load;

function mongodb_insert(server, collection, user, item, value, callback) {
	//console.log( 'Connect to [' + 'mongodb://'+server + ']');
	var conn = mongoose.createConnection('mongodb://'+server);
	conn.on('error', console.error.bind(console, 'connection error:'));
	conn.once('open', function callback () {
		var schema = conn.model( 'rec', exports.user_item_value_schema, collection);

		var rec = new schema;
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
		var schema = conn.model( 'rec', exports.user_item_value_schema, collection);
		schema.collection.insert(record_array, {w:1}, function (err) {
			if (err)
				console.log(err);
			conn.close();
			if (callback)
				callback();
		});
	});
}
