Basic Usage
========

``` 
$ cd example
$ npm inatall
$ NODE_PATH=node_modules node . -file example.input
$ time NODE_PATH=node_modules node index.js -file example.input 
[INFO] INPUT: example.input
[INFO] INPUT FILED BY: |
[INFO] TARGET MONGODB DB: localhost/test
[INFO] RAW MONGODB COLLECTION: raw
[INFO] RECOMMENDED MONGODB COLLECTION: recommendation
                [2015-01-02T10:38:05+00:00] raw import, record count: 8
[INFO] raw import, total record count: 8 @ 2015-01-02T10:38:05+00:00
[INFO] building user-item pair @ 2015-01-02T10:38:05+00:00
[INFO] building uniq user @ 2015-01-02T10:38:05+00:00
[INFO] building uniq item @ 2015-01-02T10:38:05+00:00
[INFO] building co-occurrence matrix @ 2015-01-02T10:38:05+00:00
                [2015-01-02T10:38:05+00:00] building co_matrix, item count: 22
[INFO] building recommendation @ 2015-01-02T10:38:05+00:00
[INFO] left matrix prepared, total target: 3 @ 2015-01-02T10:38:05+00:00
[INFO] right matrix prepared, total target: 5 @ 2015-01-02T10:38:05+00:00
                [2015-01-02T10:38:05+00:00] left matrix prepared, total target: 3
                [2015-01-02T10:38:05+00:00] right matrix prepared, total target: 5
                [2015-01-02T10:38:05+00:00] finish matrix multiplication



[DONE] Step 0: Reset database
[DONE] Step 1: Import data count:                       [2015-01-02T10:38:05+00:00] raw count:  8
[DONE] Step 2: build user-item based record:            [2015-01-02T10:38:05+00:00] user-item:  8
[DONE] Step 3: find uniq user:                          [2015-01-02T10:38:05+00:00] user:       3
[DONE] Step 4: find uniq item:                          [2015-01-02T10:38:05+00:00] item:       5
[DONE] Step 5&6: Item-based Co-Occurrence Matrix:       [2015-01-02T10:38:05+00:00] total co-matrix count: 22 , out: co_matrix
[DONE] Step 7: build user-item-based recommendation:    [2015-01-02T10:38:05+00:00] finish matrix multiplication
$ mongo
MongoDB shell version: 2.6.6
connecting to: test
> use test
switched to db test
> show collections
co_matrix
co_matrix_prepare
item
raw
rec_matrix_mul_usage
recommendation
system.indexes
user
user_item
user_item_filter
> db.recommendation.find()
{ "_id" : { "user" : "a", "item" : "1" }, "value" : 1.7000000000000002 }
{ "_id" : { "user" : "a", "item" : "2" }, "value" : 0.5 }
{ "_id" : { "user" : "a", "item" : "3" }, "value" : 3.6 }
{ "_id" : { "user" : "a", "item" : "4" }, "value" : 3.1 }
{ "_id" : { "user" : "a", "item" : "5" }, "value" : 0.5 }
{ "_id" : { "user" : "b", "item" : "1" }, "value" : 0.6 }
{ "_id" : { "user" : "b", "item" : "2" }, "value" : 0.8999999999999999 }
{ "_id" : { "user" : "b", "item" : "3" }, "value" : 2.1 }
{ "_id" : { "user" : "b", "item" : "4" }, "value" : 1.2 }
{ "_id" : { "user" : "b", "item" : "5" }, "value" : 0.8999999999999999 }
{ "_id" : { "user" : "c", "item" : "1" }, "value" : 0.8999999999999999 }
{ "_id" : { "user" : "c", "item" : "2" }, "value" : 0.2 }
{ "_id" : { "user" : "c", "item" : "3" }, "value" : 2 }
{ "_id" : { "user" : "c", "item" : "4" }, "value" : 1.7999999999999998 }
{ "_id" : { "user" : "c", "item" : "5" }, "value" : 0.2 }
>
```
MongoDB Sharded Cluster Usage
========

Server 1:
```
$ mkdir -p ~/d
$ mongod --port 12345 --dbpath ~/d --logpath ~/d/log
```

Server 2:
```
$ mkdir -p ~/d
$ mongod --port 12345 --dbpath ~/d --logpath ~/d/log
```

Server 3:
```
$ mkdir -p ~/c
$ mongod --port 54321 --configsvr --dbpath ~/c --logpath ~/c/log
```

Server 4:
```
$ mongos --port 30001 --configdb server3_ip:54321
```

Usage:
```
$ mongo server4_ip:30001
mongos> 

sh.addShard('server1_ip:12345')
sh.addShard('server2_ip:12345')

use test
db.dropDatabase()

db.raw.ensureIndex( { _id : "hashed" } )
db.user.ensureIndex( { _id : "hashed" } )
db.item.ensureIndex( { _id : "hashed" } )
db.user_item.ensureIndex( { _id : "hashed" } )
db.co_matrix_prepare.ensureIndex( { _id : "hashed" } )
db.co_matrix.ensureIndex( { _id : "hashed" } )
db.user_item_filter.ensureIndex( { _id : "hashed" } )
db.rec_matrix_mul_usage.ensureIndex( { _id : "hashed" } )

sh.enableSharding('test')
sh.shardCollection("test.raw", { "_id": "hashed" } )
sh.shardCollection("test.user", { "_id": "hashed" } )
sh.shardCollection("test.item", { "_id": "hashed" } )
sh.shardCollection("test.user_item", { "_id": "hashed" } )
sh.shardCollection("test.co_matrix_prepare", { "_id": "hashed" } )
sh.shardCollection("test.co_matrix", { "_id": "hashed" } )
sh.shardCollection("test.user_item_filter", { "_id": "hashed" } )
sh.shardCollection("test.rec_matrix_mul_usage", { "_id": "hashed" } )

mongos> sh.status()
--- Sharding Status ---
  sharding version: {
        "_id" : 1,
        "version" : 4,
        "minCompatibleVersion" : 4,
        "currentVersion" : 5,
        "clusterId" : ObjectId("548af39c2ab9153929359617")
}
  shards:
        {  "_id" : "shard0000",  "host" : "server1_ip:12345" }
        {  "_id" : "shard0001",  "host" : "server2_ip:12345" }
  databases:
        {  "_id" : "admin",  "partitioned" : false,  "primary" : "config" }
        {  "_id" : "test",  "partitioned" : true,  "primary" : "shard0001" }
                test.co_matrix
                        shard key: { "_id" : "hashed" }
                        chunks:
                                shard0001       7
                                shard0000       8
                        { "_id" : { "$minKey" : 1 } } -->> { "_id" : NumberLong("-7283650118201980394") } on : shard0001 Timestamp(2, 12)
                        { "_id" : NumberLong("-7283650118201980394") } -->> { "_id" : NumberLong("-6062357513476036210") } on : shard0001 Timestamp(2, 16)
                        { "_id" : NumberLong("-6062357513476036210") } -->> { "_id" : NumberLong("-4611686018427387902") } on : shard0001 Timestamp(2, 17)
                        { "_id" : NumberLong("-4611686018427387902") } -->> { "_id" : NumberLong("-3624129724690743067") } on : shard0001 Timestamp(2, 22)
                        { "_id" : NumberLong("-3624129724690743067") } -->> { "_id" : NumberLong("-2479631373787196325") } on : shard0001 Timestamp(2, 23)
                        { "_id" : NumberLong("-2479631373787196325") } -->> { "_id" : NumberLong("-1381595125143035028") } on : shard0001 Timestamp(2, 18)
                        { "_id" : NumberLong("-1381595125143035028") } -->> { "_id" : NumberLong(0) } on : shard0001 Timestamp(2, 19)
                        { "_id" : NumberLong(0) } -->> { "_id" : NumberLong("979652752207822351") } on : shard0000 Timestamp(2, 24)
                        { "_id" : NumberLong("979652752207822351") } -->> { "_id" : NumberLong("2123299225566590540") } on : shard0000 Timestamp(2, 25)
                        { "_id" : NumberLong("2123299225566590540") } -->> { "_id" : NumberLong("3205577274045542091") } on : shard0000 Timestamp(2, 20)
                        { "_id" : NumberLong("3205577274045542091") } -->> { "_id" : NumberLong("4611686018427387902") } on : shard0000 Timestamp(2, 21)
                        { "_id" : NumberLong("4611686018427387902") } -->> { "_id" : NumberLong("5590551598570865843") } on : shard0000 Timestamp(2, 26)
                        { "_id" : NumberLong("5590551598570865843") } -->> { "_id" : NumberLong("6734679053800461050") } on : shard0000 Timestamp(2, 27)
                        { "_id" : NumberLong("6734679053800461050") } -->> { "_id" : NumberLong("7960684350713852271") } on : shard0000 Timestamp(2, 14)
                        { "_id" : NumberLong("7960684350713852271") } -->> { "_id" : { "$maxKey" : 1 } } on : shard0000 Timestamp(2, 15)
                test.item
                        shard key: { "_id" : "hashed" }
                        chunks:
                                shard0001       2
                                shard0000       2
                        { "_id" : { "$minKey" : 1 } } -->> { "_id" : NumberLong("-4611686018427387902") } on : shard0001 Timestamp(2, 2)
                        { "_id" : NumberLong("-4611686018427387902") } -->> { "_id" : NumberLong(0) } on : shard0001 Timestamp(2, 3)
                        { "_id" : NumberLong(0) } -->> { "_id" : NumberLong("4611686018427387902") } on : shard0000 Timestamp(2, 4)
                        { "_id" : NumberLong("4611686018427387902") } -->> { "_id" : { "$maxKey" : 1 } } on : shard0000 Timestamp(2, 5)
                test.raw
                        shard key: { "_id" : "hashed" }
                        chunks:
                                shard0001       2
                                shard0000       2
                        { "_id" : { "$minKey" : 1 } } -->> { "_id" : NumberLong("-4611686018427387902") } on : shard0001 Timestamp(2, 2)
                        { "_id" : NumberLong("-4611686018427387902") } -->> { "_id" : NumberLong(0) } on : shard0001 Timestamp(2, 3)
                        { "_id" : NumberLong(0) } -->> { "_id" : NumberLong("4611686018427387902") } on : shard0000 Timestamp(2, 4)
                        { "_id" : NumberLong("4611686018427387902") } -->> { "_id" : { "$maxKey" : 1 } } on : shard0000 Timestamp(2, 5)
                test.user
                        shard key: { "_id" : "hashed" }
                        chunks:
                                shard0001       2
                                shard0000       2
                        { "_id" : { "$minKey" : 1 } } -->> { "_id" : NumberLong("-4611686018427387902") } on : shard0001 Timestamp(2, 2)
                        { "_id" : NumberLong("-4611686018427387902") } -->> { "_id" : NumberLong(0) } on : shard0001 Timestamp(2, 3)
                        { "_id" : NumberLong(0) } -->> { "_id" : NumberLong("4611686018427387902") } on : shard0000 Timestamp(2, 4)
                        { "_id" : NumberLong("4611686018427387902") } -->> { "_id" : { "$maxKey" : 1 } } on : shard0000 Timestamp(2, 5)
                test.user_item
                        shard key: { "_id" : "hashed" }
                        chunks:
                                shard0001       2
                                shard0000       2
                        { "_id" : { "$minKey" : 1 } } -->> { "_id" : NumberLong("-4611686018427387902") } on : shard0001 Timestamp(2, 2)
                        { "_id" : NumberLong("-4611686018427387902") } -->> { "_id" : NumberLong(0) } on : shard0001 Timestamp(2, 3)
                        { "_id" : NumberLong(0) } -->> { "_id" : NumberLong("4611686018427387902") } on : shard0000 Timestamp(2, 4)
                        { "_id" : NumberLong("4611686018427387902") } -->> { "_id" : { "$maxKey" : 1 } } on : shard0000 Timestamp(2, 5)
                test.user_item_list
                        shard key: { "_id" : "hashed" }
                        chunks:
                                shard0001       2
                                shard0000       2
                        { "_id" : { "$minKey" : 1 } } -->> { "_id" : NumberLong("-4611686018427387902") } on : shard0001 Timestamp(2, 2)
                        { "_id" : NumberLong("-4611686018427387902") } -->> { "_id" : NumberLong(0) } on : shard0001 Timestamp(2, 3)
                        { "_id" : NumberLong(0) } -->> { "_id" : NumberLong("4611686018427387902") } on : shard0000 Timestamp(2, 4)
                        { "_id" : NumberLong("4611686018427387902") } -->> { "_id" : { "$maxKey" : 1 } } on : shard0000 Timestamp(2, 5)
                test.user_prefer
                        shard key: { "_id" : "hashed" }
                        chunks:
                                shard0001       2
                                shard0000       2
                        { "_id" : { "$minKey" : 1 } } -->> { "_id" : NumberLong("-4611686018427387902") } on : shard0001 Timestamp(2, 2)
                        { "_id" : NumberLong("-4611686018427387902") } -->> { "_id" : NumberLong(0) } on : shard0001 Timestamp(2, 3)
                        { "_id" : NumberLong(0) } -->> { "_id" : NumberLong("4611686018427387902") } on : shard0000 Timestamp(2, 4)
                        { "_id" : NumberLong("4611686018427387902") } -->> { "_id" : { "$maxKey" : 1 } } on : shard0000 Timestamp(2, 5)
```

Run:
```
$ cd example
$ npm inatall
$ NODE_PATH=node_modules node . -file example.input -mongodb-db server4_ip:30001/test
```
