# Spark-StructuredStreamming-Mongo
[![CN doc](https://img.shields.io/badge/文档-中文版-blue.svg)](README.md)
[![EN doc](https://img.shields.io/badge/document-English-blue.svg)](README.en.md)
## 1.Description
read mongo by spark structureed streaming

## 2.example

```scala
object MongoStreamTest {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()
    // schema is necessary
    val schema: StructType = StructType(List(
      StructField("_id", StringType),
      StructField("BooleanTypeTest", BooleanType),
      StructField("DoubleTypeTest", DoubleType),
      StructField("Int32TypeTest", IntegerType),
      StructField("Int64TypeTest",LongType),
      StructField("NullTypeTest",NullType),
      StructField("StringTypeTest", StringType),
      StructField("ArrayTypeTest",ArrayType(StringType)),
      StructField("MapTypeTest",MapType.apply(StringType,StringType)),
      StructField("ISODateTypeTest",TimestampType),
      StructField("BinaryTypeTest",BinaryType)
    ))
    val frame: DataFrame = spark.readStream.format("mongoDb-stream")
      .schema(schema)
      .option("uri", "mongodb://ip")
      .option("database", "database")
      .load("collection")
    val query = frame.writeStream.format("mongoDb-stream")
      .option("checkpointLocation","checkpointLocation")
      .option("uri", "mongodb://ip")
      .option("database", "database")
      .start("collection")
    query.awaitTermination()
  }
}

```

