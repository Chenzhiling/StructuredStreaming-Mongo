package org.apache.spark.sql.com.chenzhiling.study.source

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.com.chenzhiling.study.util.MongoDbException
import org.apache.spark.sql.execution.streaming.{Sink, Source}
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider, StreamSourceProvider}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType


/**
 * Author: CHEN ZHI LING
 * Date: 2021/11/23
 * Description: mongo spark struct streaming realization
 */
class MongoDbStructStreaming extends StreamSourceProvider with StreamSinkProvider with DataSourceRegister{



  override def sourceSchema(sqlContext: SQLContext,
                            schema: Option[StructType],
                            providerName: String,
                            parameters: Map[String, String]): (String, StructType) = {
    if (schema.isEmpty){
      throw new MongoDbException("Table schema is not provided. Please provide the schema of the table")
    }
    (providerName,schema.get)
  }


  override def createSource(sqlContext: SQLContext,
                            metadataPath: String,
                            schema: Option[StructType],
                            providerName: String,
                            parameters: Map[String, String]): Source = {
    val options: Map[String, String] = initParams(parameters)
    new MongoDbSource(sqlContext,options,schema)
  }

  override def createSink(sqlContext: SQLContext, parameters: Map[String, String], partitionColumns: Seq[String], outputMode: OutputMode): Sink = {
    val params: Map[String, String] = initParams(parameters)
    new MongoDbSink(sqlContext,params)
  }


  override def shortName(): String = "mongoDb-stream"


  private def initParams(parameters: Map[String, String]): Map[String, String] = {
    val uri: String = parameters.getOrElse("uri", throw new IllegalArgumentException("uri must be specified ,like mongodb://localhost"))
    val database: String = parameters.getOrElse("database", throw new IllegalArgumentException("database must be specified"))
    val collection: String = checkPath(parameters)
    val params: Map[String, String] = Map[String, String](
      "uri" -> uri,
      "database" -> database,
      "collection" -> collection)
    params
  }



  private def checkPath(parameters: Map[String, String]): String = {
    parameters.getOrElse("path",
      throw new IllegalArgumentException("collection must be specified."))
  }
}
