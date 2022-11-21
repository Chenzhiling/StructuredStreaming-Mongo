import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Author: CHEN ZHI LING
 * Date: 2021/11/24
 * Description: test case
 */
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
