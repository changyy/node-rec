var	mongoose = require('mongoose'),//.set('debug', true),
	MongoClient = require('mongodb').MongoClient,
	db = exports;

exports.user_item_value_schema = new mongoose.Schema({ user: String, item: String, value: Number });

exports.uniq_target_schema = new mongoose.Schema({ _id: String});
//exports.find_uniq_target = mongodb_find_uniq_target;

exports.user_item_pair_with_sum_value_schema = new mongoose.Schema({ _id: {user: String, item: String}, value: Number });
exports.find_uniq_target_via_user_item_pair = mongodb_find_uniq_target_via_user_item_pair;

exports.user_item_list_schema = new mongoose.Schema({ _id: String, item:[] });
exports.build_user_item_pair_with_sum_value = mongodb_build_user_item_pair_with_sum_value;
//exports.build_user_item_list = mongodb_build_user_item_list;
exports.build_user_item_list_via_user_item_pair = mongodb_build_user_item_list_via_user_item_pair;

exports.item_based_co_occurrence_matrix_init_schema = new mongoose.Schema({ key: String, value: Number });
exports.build_item_based_co_occurrence_matrix_prepare = mongodb_build_item_based_co_occurrence_matrix_prepare;
exports.build_item_based_co_occurrence_matrix_prepare = mongodb_build_item_based_co_occurrence_matrix_prepare_mapreduce;
exports.build_item_based_co_occurrence_matrix = mongodb_build_item_based_co_occurrence_matrix;

exports.item_based_co_occurrence_matrix = new mongoose.Schema({ _id: String, value: Number });
//exports.build_user_prefer = mongodb_build_user_prefer;
exports.build_user_prefer_via_user_item_pair = mongodb_build_user_prefer_via_user_item_pair;
//exports.build_user_prefer_via_user_item_pair = mongodb_build_user_prefer_via_user_item_pair_mapreduce;
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
					//,
					//function (err, result) {
					//	console.log(result);
					//	db.close();
					//}
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
//*/
			});
		}
	});
/*
	var moment = require('moment');
	var conn = mongoose.createConnection('mongodb://'+server);
	conn.on('error', console.error.bind(console, 'connection error:'));
	conn.once('open', function () {
		var model = conn.model( 'raw_record', exports.user_item_value_schema, collection_in);
		var aggregate = model.aggregate(
			[
				{ 
					$group: {
						_id: {
							user: "$"+collection_in_user_field, 
							item: "$"+collection_in_item_field
						}, 
						value: {$sum: "$"+collection_in_value_field} 
					}
				},
				{
					$out: collection_out
				}
			]
		);
		aggregate.allowDiskUse(true).exec( function(err, data){
			if (err)
				console.log(err);
			var model_out = conn.model( 'record', exports.user_item_pair_with_sum_value_schema, collection_out);
			model_out.count(function(err, count){ 
				conn.close();
				if (callback) 
					callback(
						"["+moment().format() +'] '+ collection_in_user_field+"-"+collection_in_item_field + ':\t' + count
					);
			});
		});
//		var model = conn.model( 'raw_record', exports.user_item_value_schema, collection_in);
//		var aggregate = model.aggregate(
//			[
//				{ 
//					$group: {
//						_id: {
//							user: "$"+collection_in_user_field, 
//							item: "$"+collection_in_item_field
//						}, 
//						value: {$sum: "$"+collection_in_value_field} 
//					}
//				}
//			]
//		).allowDiskUse(true);//.cursor({ batchSize: 500 });
//
//		var promise = aggregate.exec();
//		promise.onFulfill(function (arg) {
//			console.log('promise.onFulfill:', arg);
//		});
//		promise.onReject(function (reason) {
//			console.log('promise.onReject:', reason);
//		});
//		promise.addBack(function (err) { 
//			console.log('promise.addBack:', err);
//			conn.close();
//		});
	});
// */
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
				//	,
				//	function (err, result) {
				//		console.log(result);
				//		db.close();
				//	}
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

/*
	var moment = require('moment');
	var conn = mongoose.createConnection('mongodb://'+server);
	conn.on('error', console.error.bind(console, 'connection error:'));
	conn.once('open', function () {
		var model = conn.model( 'user_item_pair', exports.user_item_pair_with_sum_value_schema, collection_in);
		var aggregate = model.aggregate(
			[
				{
					$group: {_id: "$_id."+collection_in_field }
				}, 
				{
					$sort: {_id: 1}
				},
				{
					$out: collection_out
				}
			]
		);
		aggregate.allowDiskUse(true).exec(
			function(err, data){
				if (err)
					console.log(err);
				var model_out = conn.model( 'record', exports.uniq_target_schema, collection_out);
				model_out.count(function(err, count){ 
					conn.close();
					if (callback) 
						callback(
							'[' + moment().format() +'] '+ collection_in_field + ':\t' + count
						);
				});
			}
		);
	});
// */
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
				//	,
				//	function (err, result) {
				//		console.log(result);
				//		db.close();
				//	}
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
/*
	var moment = require('moment');
	var conn = mongoose.createConnection('mongodb://'+server);
	conn.on('error', console.error.bind(console, 'connection error:'));
	conn.once('open', function () {
		var model = conn.model( 'raw_record', exports.user_item_value_schema, collection_in);
		var aggregate = model.aggregate(
			[
				{ 
					$group: {
						_id: "$_id."+collection_in_user_field, 
						item: {$push: "$_id."+collection_in_item_field} 
					}
				},
				{
					$out: collection_out
				}
			]
		);
		aggregate.allowDiskUse(true).exec( function(err, data){
			if (err)
				console.log(err);
			var model_out = conn.model( 'record', exports.user_item_list_schema, collection_out);
			model_out.count(function(err, count){ 
				conn.close();
				if (callback) 
					callback(
						'['+moment().format() +'] '+ collection_in_user_field+"-"+collection_in_item_field + ':\t' + count
					);
			});
		});
	});
// */
}

function mongodb_build_item_based_co_occurrence_matrix_prepare(server, collection_in, collection_in_item_field, collection_out, callback) {
	var moment = require('moment');
	var conn = mongoose.createConnection('mongodb://'+server);
	conn.on('error', console.error.bind(console, 'connection error:'));
	conn.once('open', function () {
		var model = conn.model( 'rec', exports.user_item_list_schema, collection_in);
		var stream = model.find().stream();
		var cnt = 0;
		var db_bulk_load_cnt = 100000;

		stream.on('data', function (doc) {
			stream.pause();
			console.log('\t\tco_occurrence_matrix_prepare: get doc:'+ doc[collection_in_item_field].length);
			var output_item = [];
			
			function handleRcordSelf(i, max) {
				var key = doc[collection_in_item_field][i] + "-" + doc[collection_in_item_field][i];
				cnt++;
				output_item.push( { key: key, value: 1 } );
				// RangeError: Maximum call stack size exceeded
				setTimeout( function() {
					if (i+1 < max)
						handleRecordPair(i, i+1, max);
					else
						handleFinish();
				}, 0);
			}
			function handleRecordPair(i, j, max) {
				var key = doc[collection_in_item_field][i]+"-"+doc[collection_in_item_field][j];
				output_item.push( { key: key, value: 1 } );
				key = doc[collection_in_item_field][j]+"-"+doc[collection_in_item_field][i];
				output_item.push( { key: key, value: 1 } );
				cnt += 2;

				if (output_item.length >= db_bulk_load_cnt) {
					var target = conn.model( 'target', exports.item_based_co_occurrence_matrix_init_schema, collection_out);
					target.collection.insert(output_item, {w:1}, function (err) {
						console.log("\t\t\t"+moment().format()+"\tinsert:"+output_item.length+", at ("+i+","+j+"), max:"+max);
						if (err)
							console.log(err);
						output_item = [];
						// RangeError: Maximum call stack size exceeded
						setTimeout( function() {
							if (j+ 1<max)
								handleRecordPair(i, j+1, max);
							else if (i+1<max)
								handleRcordSelf(i+1, max);
							else
								handleFinish();
						}, 0);
					});
				} else {
					// RangeError: Maximum call stack size exceeded
					setTimeout( function() {
						if (j+1<max)
							handleRecordPair(i, j+1, max);
						else if (i+1<max)
							handleRcordSelf(i+1, max);
						else
							handleFinish();
					}, 0);
				}
			}
			function handleFinish() {
				if (output_item.length) {
					var target = conn.model( 'target', exports.item_based_co_occurrence_matrix_init_schema, collection_out);
					target.collection.insert(output_item, {w:1}, function (err) {
						console.log("\t\t\t"+moment().format()+"\tfinish insert: "+output_item.length);
						if (err)
							console.log(err);
						stream.resume();
						output_item = [];
					});
				} else {
					stream.resume();
					output_item = [];
				}
			}

			// fire
			handleRcordSelf(0, doc[collection_in_item_field].length);

/*
			for (var i=0, len=doc[collection_in_item_field].length ; i<len ; ++i) {
				var key = doc[collection_in_item_field][i] + "-" + doc[collection_in_item_field][i];
				cnt++;
				output_item.push( { key: key, value: 1 } );
				for (var j = i+1 ; j<len ; ++j) {
					var key = doc[collection_in_item_field][i]+"-"+doc[collection_in_item_field][j];
					output_item.push( { key: key, value: 1 } );
					key = doc[collection_in_item_field][j]+"-"+doc[collection_in_item_field][i];
					output_item.push( { key: key, value: 1 } );
					cnt += 2;
				}
				process.stdout.write('.');
			}
			console.log('stream one doc done: '+ output_item.length);
			if (output_item.length) {
				var raw_insert = output_item;
				output_item = [];
				console.log("\tbuild_item_based_co_occurrence_matrix_prepare\t"+moment().format()+'\t:'+raw_insert.length);

				console.log("\t\twanna open new conn:"+'mongodb://'+server);
				var sub_conn = mongoose.createConnection('mongodb://'+server);
				sub_conn.on('error', console.error.bind(console, 'connection error:'));
				sub_conn.once('open', function () {
console.log('\t\t\topen success');
					var model = sub_conn.model( 'rec', exports.user_item_list_schema, collection_in);
					var target = sub_conn.model( 'target', exports.item_based_co_occurrence_matrix_init_schema, collection_out);
					target.collection.insert(raw_insert, {w:1}, function (err) {
console.log('\t\t\tinsert finish');
						if (err)
							console.log(err);
						sub_conn.close();
console.log('\t\t\t\t\t\t\tcall stream.resume();');
						stream.resume();
					});
				});
			} else {
console.log('\t\t\t\t\t\t\tcall stream.resume();');
				stream.resume();
			}
*/
		}).on('end', function () {
		}).on('error', function (err) {
			if (err)
				console.log(err);
		}).on('close', function (err) {
			if (err)
				console.log('build_item_based_co_occurrence_matrix_prepare finish error:'+err);
			conn.close();
			if (callback) 
				callback('['+moment().format() +'] build_item_based_co_occurrence_matrix_prepare:\t' + cnt);
		});

	});
}

function mongodb_build_item_based_co_occurrence_matrix_prepare_mapreduce(server, collection_in, collection_in_item_field, collection_out, callback) {
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
					var key = this[input_field][i] + "-" + this[input_field][i];
					emit(key, 1);
					cnt ++;
					for (var j = i+1 ; j<len ; ++j) {
						var key = this[input_field][i]+"-"+this[input_field][j];
						emit(key, 1);
						cnt ++;
						key = this[input_field][j]+"-"+this[input_field][i];
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

		var model = conn.model( 'rec', exports.user_item_list_schema, collection_in);
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


function mongodb_build_item_based_co_occurrence_matrix(server, collection_in, collection_in_key_field, collection_in_value_field, collection_out, callback) {
	var moment = require('moment');
	MongoClient.connect('mongodb://'+server, function (err, db) {
		if(!err) {
			var batchSize = 4096;
			db.createCollection(collection_out, {w:1}, function(err, output) {
				var cursor = db.collection(collection_in).aggregate(
					[
						{
							$group: {
								_id: "$"+collection_in_key_field, 
								value: {
									$sum: "$"+collection_in_value_field
								} 
							}
						}
					],
					{
						allowDiskUse: true,
						cursor: {
							batchSize: batchSize
						}
					}
				//	,
				//	function (err, result) {
				//		console.log(result);
				//		db.close();
				//	}
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
							'['+moment().format() +'] group by: '+ collection_in_key_field+', out: '+ collection_out +', count: ' + count
						);
				});
			});
		}
	});
// */
/*
	var conn = mongoose.createConnection('mongodb://'+server);
	conn.on('error', console.error.bind(console, 'connection error:'));
	conn.once('open', function () {
		var model = conn.model( 'rec', exports.item_based_co_occurrence_matrix_init_schema, collection_in);
		var aggregate = model.aggregate( [
			{$group: {_id: "$"+collection_in_key_field, value: {$sum: "$"+collection_in_value_field} }} ,
			{$out: collection_out}
		]);
		aggregate.allowDiskUse(true).exec(function (err, data){ 
			if (err)
				console.log(err);
			conn.close();
			if (callback) 
				callback(1);
		});
	});
// */
}

function mongodb_build_user_prefer_via_user_item_pair(server, collection_raw, collection_item, collections_matrix, collection_out, callback) {
	var conn = mongoose.createConnection('mongodb://'+server);
	conn.on('error', console.error.bind(console, 'connection error:'));
	conn.once('open', function () {
		var model_item = conn.model( 'rec', exports.uniq_target_schema, collection_item);
		var stream = model_item.find().stream();
		var db_bulk_load_cnt = 2048;
		var output_item = [];
		var moment = require('moment');
		console.log('\t\tbuild_user_prefer_via_user_item_pair @ '+ moment().format());
		stream.on('data', function (doc_item_part1) {
			stream.pause();
			var model_item_part2 = conn.model( 'rec', exports.uniq_target_schema, collection_item);
			var stream_part2 = model_item_part2.find().stream();
			stream_part2.on('data', function (doc_item_part2) {
				stream_part2.pause();
				var key = doc_item_part1._id + "-" + doc_item_part2._id;
				var model_part3 = conn.model( 'rec', exports.item_based_co_occurrence_matrix, collections_matrix);
				model_part3.findOne( { _id: key }, function (err, doc3) {
					if (err)
						console.log(err);
					if (doc3 && doc3.value) {
						var model_user = conn.model( 'user', exports.uniq_target_schema, 'user');
						stream_user = model_user.find().stream();
						stream_user.on('data', function(user_info) {
							stream_user.pause();
							var model_raw = conn.model( 'raw_record', exports.user_item_pair_with_sum_value_schema, collection_raw);
							model_raw.findOne().where('_id.user').equals(user_info._id).where('_id.item').equals(doc_item_part2._id).exec(function (err, user_item) {
								if (user_item) {
									output_item.push({user: user_info._id,item: doc_item_part1._id, value: doc3.value * user_item.value });
									if (output_item.length >= db_bulk_load_cnt) {
										console.log("\t\t\t"+moment().format()+'\t:'+output_item.length);
										var model_user_prefer = conn.model( 'user_prefer', exports.user_item_value_schema, collection_out);
										model_user_prefer.collection.insert(output_item, {w:1}, function (err) {
											output_item = [];
											if (err)
												console.log(err);
											stream_user.resume();
										});
									} else {
										stream_user.resume();
									}
								} else {
									stream_user.resume();
								}
							});
							
						}).on('end', function () {
						}).on('close', function (err) {
							stream_part2.resume();
						});
					} else {
						stream_part2.resume();
					}
				});
			}).on('end', function () { 
			}).on('close', function (err) {
				stream.resume();
			});
		}).on('end', function () {
		}).on('error', function (err) {
			if (err)
				console.log(err);
		}).on('close', function (err) {
			if (err)
				console.log('build_user_prefer finish error:'+err);
			if (output_item.length) {
				console.log("\t\t\t"+moment().format()+'\tfinish: '+output_item.length);
				var model_user_prefer = conn.model( 'user_prefer', exports.user_item_value_schema, collection_out);
				model_user_prefer.collection.insert(output_item, {w:1}, function (err) {
					output_item = [];
					if (err)
						console.log(err);
					conn.close();
					if (callback) 
						callback('['+moment().format()+']');
				});
			} else {
				conn.close();
				if (callback) 
					callback('['+moment().format()+']');
			}
		});
	});
}

function mongodb_build_user_prefer_via_user_item_pair_mapreduce(server, collection_raw, collection_item, collections_matrix, collection_out, callback) {
	var conn = mongoose.createConnection('mongodb://'+server);
	conn.on('error', console.error.bind(console, 'connection error:'));
	conn.once('open', function () {
		var model_item = conn.model( 'rec', exports.uniq_target_schema, collection_item);
		var stream = model_item.find().stream();
		var db_bulk_load_cnt = 2048;
		var output_item = [];
		var moment = require('moment');
		console.log('\t\tbuild_user_prefer_via_user_item_pair @ '+ moment().format());
		stream.on('data', function (doc_item_part1) {
			stream.pause();
			var model_item_part2 = conn.model( 'rec', exports.uniq_target_schema, collection_item);
			var stream_part2 = model_item_part2.find().stream();
			stream_part2.on('data', function (doc_item_part2) {
				stream_part2.pause();
				var key = doc_item_part1._id + "-" + doc_item_part2._id;
				var model_part3 = conn.model( 'rec', exports.item_based_co_occurrence_matrix, collections_matrix);
				model_part3.findOne( { _id: key }, function (err, doc3) {
					if (err)
						console.log(err);
					if (doc3 && doc3.value) {
						var model_user = conn.model( 'user', exports.uniq_target_schema, 'user');
						stream_user = model_user.find().stream();
						stream_user.on('data', function(user_info) {
							stream_user.pause();
							var model_raw = conn.model( 'raw_record', exports.user_item_pair_with_sum_value_schema, collection_raw);
							model_raw.findOne().where('_id.user').equals(user_info._id).where('_id.item').equals(doc_item_part2._id).exec(function (err, user_item) {
								if (user_item) {
									output_item.push({user: user_info._id,item: doc_item_part1._id, value: doc3.value * user_item.value });
									if (output_item.length >= db_bulk_load_cnt) {
										console.log("\t\t\t"+moment().format()+'\t:'+output_item.length);
										var model_user_prefer = conn.model( 'user_prefer', exports.user_item_value_schema, collection_out);
										model_user_prefer.collection.insert(output_item, {w:1}, function (err) {
											output_item = [];
											if (err)
												console.log(err);
											stream_user.resume();
										});
									} else {
										stream_user.resume();
									}
								} else {
									stream_user.resume();
								}
							});
							
						}).on('end', function () {
						}).on('close', function (err) {
							stream_part2.resume();
						});
					} else {
						stream_part2.resume();
					}
				});
			}).on('end', function () { 
			}).on('close', function (err) {
				stream.resume();
			});
		}).on('end', function () {
		}).on('error', function (err) {
			if (err)
				console.log(err);
		}).on('close', function (err) {
			if (err)
				console.log('build_user_prefer finish error:'+err);
			if (output_item.length) {
				console.log("\t\t\t"+moment().format()+'\tfinish: '+output_item.length);
				var model_user_prefer = conn.model( 'user_prefer', exports.user_item_value_schema, collection_out);
				model_user_prefer.collection.insert(output_item, {w:1}, function (err) {
					output_item = [];
					if (err)
						console.log(err);
					conn.close();
					if (callback) 
						callback('['+moment().format()+']');
				});
			} else {
				conn.close();
				if (callback) 
					callback('['+moment().format()+']');
			}
		});
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
				//	,
				//	function (err, result) {
				//		console.log(result);
				//		db.close();
				//	}
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
/*
	var conn = mongoose.createConnection('mongodb://'+server);
	conn.on('error', console.error.bind(console, 'connection error:'));
	conn.once('open', function () {
		var model = conn.model( 'rec', exports.user_item_value_schema, collection_in);
		var aggregate = model.aggregate( [
			{$group: {_id: { user: "$"+collection_in_user_field, item: "$"+collection_in_item_field}, value: {$sum: "$"+collection_in_value_field} }} ,
			{$sort: {_id: 1} },
			{$out: collection_out}
		]);
		aggregate.allowDiskUse(true).exec(function (err, data){ 
			if (err)
				console.log('build_recommendation errro:', err);
			conn.close();
			if (callback) 
				callback(1);
		});
	});
// */
}

//
// slow version
//
function mongodb_find_uniq_target(server, collection_in, collection_in_field, collection_out, callback) {
	var moment = require('moment');
	var conn = mongoose.createConnection('mongodb://'+server);
	conn.on('error', console.error.bind(console, 'connection error:'));
	conn.once('open', function () {
		var model = conn.model( 'raw_record', exports.user_item_value_schema, collection_in);
		var aggregate = model.aggregate(
			[
				{
					$group: {_id: "$"+collection_in_field }
				}, 
				{
					$sort: {_id: 1}
				},
				{
					$out: collection_out
				}
			]
		);
		aggregate.allowDiskUse(true).exec(
			function(err, data){
				if (err)
					console.log(err);
				//conn.close();
				//if (callback) 
				//	callback(moment().format());
				var model_out = conn.model( 'record', exports.uniq_target_schema, collection_out);
				model_out.count(function(err, count){ 
					conn.close();
					if (callback) 
						callback(
						//{
						//	field: collection_in_field,
						//	count: count,
						//	finish: moment().format()
						//}
							'[' + moment().format() +'] '+ collection_in_field + ':\t' + count
						);
				});
				/*
				if (err) {
					console.log(err);
					conn.close();
					if (callback) 
						callback(0);
				} else {
					var target = conn.model( 'target', exports.uniq_target_schema, collection_out);
					target.collection.insert(data, {w:1}, function (err) {

						if (err)
							console.log(err);
						conn.close();
						if (callback) 
							callback(data.length);
					});
				}
				*/
			}
		);
	});
}

function mongodb_build_user_item_list(server, collection_in, collection_in_user_field, collection_in_item_field, collection_out, callback) {
	var moment = require('moment');
	var conn = mongoose.createConnection('mongodb://'+server);
	conn.on('error', console.error.bind(console, 'connection error:'));
	conn.once('open', function () {
		var model = conn.model( 'raw_record', exports.user_item_value_schema, collection_in);
		var aggregate = model.aggregate(
			[
				{ 
					$group: {
						_id: "$"+collection_in_user_field, 
						item: {$push: "$"+collection_in_item_field} 
					}
				},
				{
					$out: collection_out
				}
			]
		);
		aggregate.allowDiskUse(true).exec( function(err, data){
			if (err)
				console.log(err);
			var model_out = conn.model( 'record', exports.user_item_list_schema, collection_out);
			model_out.count(function(err, count){ 
				conn.close();
				if (callback) 
					callback(
						'['+moment().format() +'] '+ collection_in_user_field+"-"+collection_in_item_field + ':\t' + count
					);
			});
		});
	});
}

function mongodb_build_user_prefer(server, collection_raw, collection_item, collections_matrix, collection_out, callback) {
	var conn = mongoose.createConnection('mongodb://'+server);
	conn.on('error', console.error.bind(console, 'connection error:'));
	conn.once('open', function () {
		var model_item = conn.model( 'rec', exports.uniq_target_schema, collection_item);
		var stream = model_item.find().stream();
		var db_bulk_load_cnt = 10000;
		var output_item = [];
		var moment = require('moment');
		stream.on('data', function (doc_item_part1) {
			stream.pause();
			var model_item_part2 = conn.model( 'rec', exports.uniq_target_schema, collection_item);
			var stream_part2 = model_item_part2.find().stream();
			stream_part2.on('data', function (doc_item_part2) {
				stream_part2.pause();
				var key = doc_item_part1._id + "-" + doc_item_part2._id;
				var model_part3 = conn.model( 'rec', exports.item_based_co_occurrence_matrix, collections_matrix);
				model_part3.findOne( { _id: key }, function (err, doc3) {
					if (err)
						console.log(err);
					if (doc3 && doc3.value) {
						var model_user = conn.model( 'user', exports.uniq_target_schema, 'user');
						stream_user = model_user.find().stream();
						stream_user.on('data', function(user_info) {
							stream_user.pause();
							var model_raw = conn.model( 'raw_record', exports.user_item_value_schema, collection_raw);
							model_raw.findOne( {user: user_info._id, item:doc_item_part2._id} , function (err, user_item) {
								if (user_item) {
									output_item.push({user: user_info._id,item: doc_item_part1._id, value: doc3.value * user_item.value });
									if (output_item.length >= db_bulk_load_cnt) {
										console.log("\t\tbuild_user_prefer\t"+moment().format()+'\t:'+output_item.length);
										var model_user_prefer = conn.model( 'user_prefer', exports.user_item_value_schema, collection_out);
										model_user_prefer.collection.insert(output_item, {w:1}, function (err) {
											output_item = [];
											if (err)
												console.log(err);
											stream_user.resume();
										});
									} else {
										stream_user.resume();
									}
								} else {
									stream_user.resume();
								}
							});
							
						}).on('end', function () {
						}).on('close', function (err) {
							stream_part2.resume();
						});
					} else {
						stream_part2.resume();
					}
				});
			}).on('end', function () { 
			}).on('close', function (err) {
				stream.resume();
			});
		}).on('end', function () {
			console.log( 'end' );
		}).on('error', function (err) {
			if (err)
				console.log(err);
		}).on('close', function (err) {
			if (err)
				console.log('build_user_prefer finish error:'+err);
			if (output_item.length) {
				console.log("\tbuild_user_prefer\t"+moment().format()+'\t finish:'+output_item.length);
				var model_user_prefer = conn.model( 'user_prefer', exports.user_item_value_schema, collection_out);
				model_user_prefer.collection.insert(output_item, {w:1}, function (err) {
					output_item = [];
					if (err)
						console.log(err);
					conn.close();
					if (callback) 
						callback(1);
				});
			} else {
				conn.close();
				if (callback) 
					callback(1);
			}
		});
	});
}

