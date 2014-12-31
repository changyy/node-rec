var	mongoose = require('mongoose'),//.set('debug', true),
	MongoClient = require('mongodb').MongoClient,
	db = exports;

exports.schema_user_item_value = new mongoose.Schema({ user: String, item: String, value: Number });
exports.schema_uniq_target = new mongoose.Schema({ _id: String });
exports.schema_user_item_pair_with_sum_value = new mongoose.Schema({ _id: {user: String, item: String}, value: Number });

exports.find_uniq_target_via_user_item_pair = mongodb_find_uniq_target_via_user_item_pair;

exports.schema_user_item_list = new mongoose.Schema({ _id: String, item:[] });
exports.build_user_item_pair_with_sum_value = mongodb_build_user_item_pair_with_sum_value;
exports.build_user_item_list_via_user_item_pair = mongodb_build_user_item_list_via_user_item_pair;
exports.build_item_based_co_occurrence_matrix = mongodb_build_item_based_co_occurrence_matrix;

exports.schema_item_based_co_occurrence_matrix = new mongoose.Schema({ _id: String, value: Number });
exports.schema_matrix_multiplication = new mongoose.Schema({ _id: {}, value: Number });
exports.build_user_prefer_via_user_item_pair = mongodb_build_user_prefer_via_user_item_pair;
exports.build_recommendation = mongodb_build_recommendation;

function mongodb_build_user_item_pair_with_sum_value(server, collection_in, collection_in_user_field, collection_in_item_field, collection_in_value_field, collection_out, callback) {
	var moment = require('moment');
	MongoClient.connect('mongodb://'+server, function (err, db) {
		if(!err) {
			var batchSize = 4096;
			db.createCollection(collection_out, {w:1}, function(err, output) {
				var cursor = db.collection(collection_in).aggregate([
						{ 
							$group: {
								_id: {
									user: "$"+collection_in_user_field, 
									item: "$"+collection_in_item_field
								}, 
								value: {$sum: "$"+collection_in_value_field} 
							}
						}
					],
					{
						allowDiskUse: true,
						cursor: {
							batchSize: batchSize
						}
					}
				);
				var output_items = [];
				var count = 0;
				cursor.on('data', function(doc) {
					output_items.push(doc);
					if (output_items.length > batchSize) {
						cursor.pause();
						output.insert(output_items, function(err, records){
							output_items = [];
							cursor.resume();
						});
					}
					count++;
				}).on('end', function() {
					if (output_items.length) {
						output.insert(output_items, function(err, records){
							output_items = [];
							cursor.resume();
						});
					}
					db.close();
					if (callback) 
						callback(
							"["+moment().format() +'] '+ collection_in_user_field+"-"+collection_in_item_field + ':\t' + count
						);
				});
			});
		}
	});
}

function mongodb_find_uniq_target_via_user_item_pair(server, collection_in, collection_in_field, collection_out, callback) {
	var moment = require('moment');
	MongoClient.connect('mongodb://'+server, function (err, db) {
		if(!err) {
			var batchSize = 4096;
			db.createCollection(collection_out, {w:1}, function(err, output) {
				var cursor = db.collection(collection_in).aggregate([
						{
							$group: {_id: "$_id."+collection_in_field }
						}, 
						{
							$sort: {_id: 1}
						}
					],
					{
						allowDiskUse: true,
						cursor: {
							batchSize: batchSize
						}
					}
				);
				var output_items = [];
				var count = 0;
				cursor.on('data', function(doc) {
					output_items.push(doc);
					if (output_items.length > batchSize) {
						cursor.pause();
						output.insert(output_items, function(err, records){
							output_items = [];
							cursor.resume();
						});
					}
					count++;
				}).on('end', function() {
					if (output_items.length) {
						output.insert(output_items, function(err, records){
							output_items = [];
							cursor.resume();
						});
					}
					db.close();
					if (callback) 
						callback(
							'[' + moment().format() +'] '+ collection_in_field + ':\t' + count
						);
				});
			});
		}
	});
}

function mongodb_build_user_item_list_via_user_item_pair(server, collection_in, collection_in_user_field, collection_in_item_field, collection_out, callback) {
	var moment = require('moment');
	MongoClient.connect('mongodb://'+server, function (err, db) {
		if(!err) {
			var batchSize = 4096;
			db.createCollection(collection_out, {w:1}, function(err, output) {
				var cursor = db.collection(collection_in).aggregate(
					[
						{ 
							$group: {
								_id: "$_id."+collection_in_user_field, 
								item: {$push: "$_id."+collection_in_item_field} 
							}
						}
					],
					{
						allowDiskUse: true,
						cursor: {
							batchSize: batchSize
						}
					}
				);
				var output_items = [];
				var count = 0;
				cursor.on('data', function(doc) {
					output_items.push(doc);
					if (output_items.length > batchSize) {
						cursor.pause();
						output.insert(output_items, function(err, records){
							output_items = [];
							cursor.resume();
						});
					}
					count++;
				}).on('end', function() {
					if (output_items.length) {
						output.insert(output_items, function(err, records){
							output_items = [];
							cursor.resume();
						});
					}
					db.close();
					if (callback) 
						callback(
							'['+moment().format() +'] '+ collection_in_user_field+"-"+collection_in_item_field + ':\t' + count
						);
				});
			});
		}
	});
}

function mongodb_build_item_based_co_occurrence_matrix(server, collection_in, collection_in_item_field, collection_out, callback) {
	var moment = require('moment');
	var conn = mongoose.createConnection('mongodb://'+server);
	conn.on('error', console.error.bind(console, 'connection error:'));
	conn.once('open', function () {
		var o = {};
		o.scope = { input_field: collection_in_item_field };
		o.verbose = true;
		o.jsMode = true;
		o.map = function(){
			if (this[input_field]) {
				var cnt = 0;
				for (var i=0, len=this[input_field].length ; i<len ; ++i) {
					var key = { r: this[input_field][i], c: this[input_field][i] };
					//var key = this[input_field][i] + '-' + this[input_field][i];
					emit(key, 1);
					cnt ++;
					for (var j = i+1 ; j<len ; ++j) {
						var key = { r: this[input_field][i], c: this[input_field][j] };
						//var key = this[input_field][i] + '-' + this[input_field][j];
						emit(key, 1);
						cnt ++;
						key = { r: this[input_field][j], c: this[input_field][i] };
						//key = this[input_field][j] + '-' + this[input_field][i];
						emit(key, 1);
						cnt ++;
					}
				}
			}
		}
		o.reduce = function(k, vals) {
			return Array.sum(vals);
		}
		o.out = {
			replace: collection_out, 
			sharded: true,
		};

		var model = conn.model( 'rec', exports.schema_user_item_list, collection_in);
		model.mapReduce(o, function (err, results) {
			if(err)
				console.log(err);
			conn.close();
			if (callback) 
				callback(
					'['+moment().format() +'] select: '+ collection_in_item_field + ', out: ' + collection_out
				);
		});
	});
}

function mongodb_build_user_prefer_via_user_item_pair(server, collection_user, collection_item, collection_user_item, collections_matrix, collection_out_prepare, collection_out, callback) {
	var moment = require('moment');
	var async = require('async');
	async.series([

		// STEP 1: prepare left matrix
		function(cal) {
			var conn = mongoose.createConnection('mongodb://'+server);
			conn.on('error', console.error.bind(console, 'connection error:'));
			conn.once('open', function () {
				var moment = require('moment');
		
				// prepare left matrix
				var model_uniq_user = conn.model( 'user', exports.schema_uniq_target, collection_user);
				var stream = model_uniq_user.find().stream();
				var user = [];
				var max_user_per_mapreduce = 128;
				var total_target = 0;
		
				var o = {};
				o.scope = { 
					target: [],
					out_matrix_type: 'left',
					in_matrix_row: 'r',
					in_matrix_column: 'c',
					in_matrix_value: 'value',
				};
				o.verbose = true;
				o.jsMode = true;
				o.map = function(){
					if (
						this['_id'] != undefined
						&& this['_id'][in_matrix_row] != undefined 
						&& this['_id'][in_matrix_column] != undefined
						&& this[in_matrix_value] != undefined
					) {
						for (var i in target) {
							var key = { 
								type: out_matrix_type,
								user: target[i],
								item: this['_id'][in_matrix_row],
								index: this['_id'][in_matrix_column]
							};
							emit(key, this[in_matrix_value]);
						}
					}
				}
				o.out = {
					replace: collection_out_prepare,
					sharded: true,
				};
		
				stream.on('data', function(doc) {
					user.push(doc._id);
					total_target++;
					if (user.length > max_user_per_mapreduce) {
						stream.pause();
						o.scope.target = user;
						var sub_conn = mongoose.createConnection('mongodb://'+server);
						sub_conn.on('error', console.error.bind(console, 'connection error:'));
						sub_conn.once('open', function () {
							model_co_matrix = sub_conn.model( 'co_matrix', exports.schema_item_based_co_occurrence_matrix, collections_matrix);
							model_co_matrix.mapReduce(o, function (err, results) {
								sub_conn.close();
								user = [];
								o.scope.target = [];
								stream.resume();
							});
						});
					}
				}).on('end', function() {
					if (user.length > 0) {
						stream.pause();
						o.scope.target = user;
						var sub_conn = mongoose.createConnection('mongodb://'+server);
						sub_conn.on('error', console.error.bind(console, 'connection error:'));
						sub_conn.once('open', function () {
							model_co_matrix = sub_conn.model( 'co_matrix', exports.schema_item_based_co_occurrence_matrix, collections_matrix);
							model_co_matrix.mapReduce(o, function (err) {
								if(err)
									console.log(err);
								sub_conn.close();
								user = [];
								o.scope.target = [];
								stream.resume();
							});
						});
					}
				}).on('error', function() {
		
				}).on('close', function() {
					conn.close();
					cal(null, '\t['+moment().format()+'] finish import left matrix, total target: '+ total_target);
				});
			});
		},

		// STEP 2: prepare right matrix
		function(cal) {
			var conn = mongoose.createConnection('mongodb://'+server);
			conn.on('error', console.error.bind(console, 'connection error:'));
			conn.once('open', function () {
				var moment = require('moment');
		
				// prepare right matrix
				var model_uniq_item = conn.model( 'item', exports.schema_uniq_target, collection_item);
				var stream = model_uniq_item.find().stream();
				var item = [];
				var max_item_per_mapreduce = 128;
				var total_target = 0;
		
				var o = {};
				o.scope = { 
					target: [],
					out_matrix_type: 'right',
					in_matrix_row: 'item',
					in_matrix_column: 'user',
					in_matrix_value: 'value',
				};
				o.verbose = true;
				o.jsMode = true;
				o.out = {
					merge: collection_out_prepare,
					sharded: true,
				};
				o.map = function(){
					if (
						this['_id'] != undefined
						&& this['_id'][in_matrix_row] != undefined 
						&& this['_id'][in_matrix_column] != undefined
						&& this[in_matrix_value] != undefined
					) {
						for (var i in target) {
							var key = { 
								type: out_matrix_type,
								user: this['_id'][in_matrix_column],
								item: target[i],
								index: this['_id'][in_matrix_row]
							};
							emit(key, this[in_matrix_value]);
						}
					}
				}
		
				stream.on('data', function(doc) {
					item.push(doc._id);
					total_target++;
					if (item.length > max_item_per_mapreduce) {
						stream.pause();
						o.scope.target = item;
						var sub_conn = mongoose.createConnection('mongodb://'+server);
						sub_conn.on('error', console.error.bind(console, 'connection error:'));
						sub_conn.once('open', function () {
							model_user_item_value = conn.model( 'user_item_value', exports.schema_user_item_value, collection_user_item);
							model_user_item_value.mapReduce(o, function (err, results) {
								sub_conn.close();
								item = [];
								o.scope.target = [];
								stream.resume();
							});
						});
					}
				}).on('end', function() {
					if (item.length > 0) {
						stream.pause();
						o.scope.target = item;
						var sub_conn = mongoose.createConnection('mongodb://'+server);
						sub_conn.on('error', console.error.bind(console, 'connection error:'));
						sub_conn.once('open', function () {
							model_user_item_value = sub_conn.model( 'user_item_value', exports.schema_user_item_value, collection_user_item);
							model_user_item_value.mapReduce(o, function (err, results) {
								if(err)
									console.log(err);
								sub_conn.close();
								item = [];
								o.scope.target = [];
								stream.resume();
							});
						});
					}
				}).on('error', function() {
		
				}).on('close', function() {
					conn.close();
					cal(null, '\t['+moment().format()+'] finish import right matrix, total target: '+ total_target);
				});
			});

		},

		// STEP 3: matrix multiplication
		function(cal) {
			var conn = mongoose.createConnection('mongodb://'+server);
			conn.on('error', console.error.bind(console, 'connection error:'));
			conn.once('open', function () {
				var o = {};
				o.scope = { 
					matrix_typeFieldName: 'type',
					matrix_userFieldName: 'user',
					matrix_itemFieldName: 'item',
					matrix_indexFieldName: 'index',
					matrix_valueFieldName: 'value',

					leftMatrixType: 'left',
					rightMatrixType: 'right',
				}
				o.jsMode = true;
				o.map = function() {
					var key = {}
					key[matrix_userFieldName] = this['_id'][matrix_userFieldName];
					key[matrix_itemFieldName] = this['_id'][matrix_itemFieldName];
					var val = {}
					val[matrix_indexFieldName] = this['_id'][matrix_indexFieldName];
					val[matrix_valueFieldName] = this[matrix_valueFieldName];
					emit(key, val);
				}
				o.reduce = function(k, vals) {
//print(JSON.stringify(k));
//print(JSON.stringify(vals));
//return;
					var sum = 0;
					var mul = {};
					vals.forEach(function(doc){ 
						if (mul[doc[matrix_indexFieldName]] != undefined) 
							sum += mul[doc[matrix_indexFieldName]] * doc[matrix_valueFieldName];
						else
							mul[doc[matrix_indexFieldName]] = doc[matrix_valueFieldName];
					});
					return sum;
				}
				o.out = {
					replace: collection_out,
					sharded: true,
				}
				model_prepare = conn.model( 'matrix_prepare', exports.schema_matrix_multiplication, collection_out_prepare);
				model_prepare.mapReduce(o, function(err, results) {
					if (err)
						console.log(err);
					conn.close();
					if (cal) 
						cal(null, '\t['+moment().format()+'] finish matrix multiplication');
				});
			});
		}
	], function(err, result){
		if (err)
			console.log(err);
		for (var i=0, cnt=result.length ; i<cnt ; ++i)
			console.log(result[i]);

		if (callback) 
			callback('['+moment().format()+'] finish build_user_prefer');
	});
}

function mongodb_build_recommendation(server, collection_in, collection_in_user_field, collection_in_item_field, collection_in_value_field, collection_out, callback) {
	var moment = require('moment');
	MongoClient.connect('mongodb://'+server, function (err, db) {
		if(!err) {
			var batchSize = 4096;
			db.createCollection(collection_out, {w:1}, function(err, output) {
				var cursor = db.collection(collection_in).aggregate(
					[
						{
							$group: {
								_id: { 
									user: "$"+collection_in_user_field, 
									item: "$"+collection_in_item_field
								}, 
								value: {
									$sum: "$"+collection_in_value_field
								} 
							}
						} ,
						{
							$sort: {
								_id: 1
							} 
						}
					],
					{
						allowDiskUse: true,
						cursor: {
							batchSize: batchSize
						}
					}
				);
				var output_items = [];
				var count = 0;
				cursor.on('data', function(doc) {
					output_items.push(doc);
					if (output_items.length > batchSize) {
						cursor.pause();
						output.insert(output_items, function(err, records){
							output_items = [];
							cursor.resume();
						});
					}
					count++;
				}).on('end', function() {
					if (output_items.length) {
						output.insert(output_items, function(err, records){
							output_items = [];
							cursor.resume();
						});
					}
					db.close();
					if (callback) 
						callback(
							'['+moment().format() +'] '+ collection_in_user_field+"-"+ collection_in_item_field + ':\t' + count
						);
				});
			});
		}
	});
}
