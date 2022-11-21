# Spark-StructuredStreaming-Mongo

## 1. 介绍

最近，有一个新需求，需要将mongo某个表的增量数据，实时同步到数据湖仓库中，于是自定义了这么一个数据源。

mongoDb-stream数据源，通过扩展Spark Structured Streaming的Source和Sink实现对mongoDb数据库的流读和流写功能。

使用需要通过option传入mongo-ip，数据库名，集合名。

流处理读取还须指定schema，流处理回写入mongo时可以不使用。

只支持append增量模式

## 2. 实现逻辑

1. 继承StreamSourceProvider和StreamSinkProvider接口
2. 继承source和sink创建自定义的source和sink
3. 重写source和sink的方法

### 2.1 Source类实现

自定义source需要实现4个方法，分别是：

- schema
- getOffset
- getBatch
- stop

#### 2.1.1 schema方法

与指定的schema方法

#### 2.1.2 getOffset方法

spark不断执行轮循，获取数据的偏移量，这里参考了网上别人实现mysql的的方法，链接在最下面

通过countDocuments()方法，获取最新的数据条数，所以现在只支持append模式，update,delete等不会生效

```scala
def getLatestOffset: Map[String, Long] = {
    val db: MongoDatabase = client.getDatabase(database)
    val count: Long = db.getCollection(collectionName).countDocuments()
    Map[String, Long](collectionName -> count)
}
```

#### 2.1.4 getBatch方法

getBatch方法需要返回一个DataFrame，所以需要将两次轮循过程中新增的数据，转变成DataFrame。

具体过程 ：Mongo-Document ==> Iterator[InternalRow] ==> Rdd[InternalRow] ==> DataFrame

1. 根据偏移量获取新增的mongo数据，返回一个迭代器

   ```scala
   val documents: FindIterable[BsonDocument] = collection.find(classOf[BsonDocument]).limit(limit.toInt).skip(offset.toInt)
   val iterator: MongoCursor[BsonDocument] = documents.iterator()
   ```

2. 将BsonDocument转换成包含InternalRow的迭代器，通过重写spark NextIterator[U]抽象类实现

   ```scala
   /**
      * change document to InternalRow
      * @param iterator Mongo[InternalRow]
      * @param schema schema
      * @return
      */
     def BsonDocumentToInternalRow(iterator:MongoCursor[BsonDocument],schema:StructType): Iterator[InternalRow] = {
       new NextIterator[InternalRow] {
         private[this] val rs: MongoCursor[BsonDocument] = iterator
   
         override protected def getNext(): InternalRow = {
           if (iterator.hasNext){
             val document: BsonDocument = rs.next()
             MongoDbConvertor.documentToInternalRow(document, schema)
           }else{
             finished = true
             null.asInstanceOf[InternalRow]
           }
         }
   
         override protected def close(): Unit = {
           try {
             rs.close()
           } catch {
             case e: Exception => logWarning("Exception closing MongoCursor",e)
           }
         }
       }
     }
   ```

   这里还涉及到BsonDocument的数据类型和Spark InternalRow的数据类型转换。

   使用了部分Mongo-Spark-Connector包中的代码，其中MapFunctions类中的documentToRow方法可以将document数据变成Row，但代码需要返回的是InternalRow类型。同时Row的部分数据类型，相比于InternalRow的是不同的，比如Row字符串类型使用String，而InternalRow字符串类型使用UTF8String。

   所以对包括Map,Array,String,TimesStamp等类型的类型转换方法进行了修改，具体可以参考代码。

3. Iterator变成Rdd

   ```scala
   val rdd: RDD[InternalRow] = SQLContext.sparkContext.parallelize(rows)
   ```

4. Rdd变成DataFrame

   ```scala
   val frame: DataFrame = SQLContext.internalCreateDataFrame(rdd, schema, isStreaming = true)
   ```

#### 2.1.5 stop方法

关闭mongo的客户端

### 2.2 Sink类实现

- 重写addBatch方法

使用了mongo-spark-connector.jar中的方法，拼接uri，将mongo数据写出

```scala
class MongoDbSink(sqlContext: SQLContext, options: Map[String, String]) extends Sink with Logging {


  val uri: String = options("uri")
  val database: String = options("database")
  val collectionName: String = options("collection")
  lazy val path: String = uri + "/" + database + "." + collectionName

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    val query: QueryExecution = data.queryExecution
    val rdd: RDD[InternalRow] = query.toRdd
    val df: DataFrame = sqlContext.internalCreateDataFrame(rdd, data.schema)
    df.write.format("mongo")
      .mode(SaveMode.Append)
      .option("spark.mongodb.output.uri",path)
      .save()
  }
}

```

## 3. 使用说明

### 3.1 流读

- 指定mongo地址，数据库名，集合名

```scala
val uri = "mongodb://ip"
val database = "database"
val collection = "collection"
```

- 按集合数据字段名称和类型构建schema

```scala
val schema = StructType(List(
    StructField("_id", StructType(List(StructField("oid",StringType)))),
    StructField("BooleanTypeTest", BooleanType),
    StructField("Int32TypeTest", IntegerType),
    StructField("Int64TypeTest",LongType),
    StructField("DoubleTypeTest", DoubleType),
    StructField("NullTypeTest",NullType),
    StructField("BinaryTypeTest",BinaryType),
    StructField("StringTypeTest", StringType),
    StructField("ArrayTypeTest",ArrayType(StringType)),
    StructField("MapTypeTest",MapType.apply(StringType,StringType)),
    StructField("ISODateTypeTest",TimestampType)
))
```

- 读取并打印到控制台

```scala
val frame = spark.readStream.format("mongoDb-stream").schema(schema).option("uri", uri).option("database", database).load(collection)
val query = frame.writeStream.format("console").start()
```

### 3.2 流写

- 指定mongo地址，数据库名，集合名，checkpointLocation

```scala
val uri = "mongodb://ip"
val database = "database"
val collection = "collection"
val checkpointLocation = "/tmp/mongo/"
```

- 写入

```scala
val query = frame.writeStream.format("mongoDb-stream").option("uri",uri).option("database",database).option("path", collection).option("checkpointLocation", checkpointLocation).start()
```

## 4. mongo字段和spark-schema字段对应关系

列举了部分常用mongo字段所对应的spark-schema字段类型。没有列举的，可以通过代码查看。

| mongo字段类型 | spark-schema类型                                    |
| ------------- | --------------------------------------------------- |
| Object_id     | StringType                                          |
| Boolean       | BooleanType                                         |
| Int32         | IntegerType                                         |
| Int64         | LongType                                            |
| Double        | DoubleType                                          |
| Null          | NullType                                            |
| BinData       | BinaryType                                          |
| String        | StringType                                          |
| Array         | ArrayType                                           |
| ISODate       | DateType，Timestamp                                 |
| BsonTimeStamp | StringType                                          |
| Maxkey        | StructType(List(StructField("maxKey",IntegerType))) |
| Minkey        | StructType(List(StructField("minKey",IntegerType))) |

## 5. 仓库地址

https://gitee.com/zhiling-chen/spark-structured-streamming-mongo

## 6. 参考

https://blog.csdn.net/shirukai/article/details/86687672?spm=1001.2014.3001.5502
