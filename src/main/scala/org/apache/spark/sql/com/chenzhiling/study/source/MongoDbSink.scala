package org.apache.spark.sql.com.chenzhiling.study.source

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

/**
 * Author: CHEN ZHI LING
 * Date: 2021/11/23
 * Description: use mongo-spark-connector save dataframe to mongo
 */
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

