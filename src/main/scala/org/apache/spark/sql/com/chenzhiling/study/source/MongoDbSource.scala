package org.apache.spark.sql.com.chenzhiling.study.source

import com.mongodb.client._
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.com.chenzhiling.study.util.MongoDbUtils
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.bson.{BsonDocument, Document}
import org.json4s.jackson.Serialization
import org.json4s.{Formats, NoTypeHints}

/**
 * Author: CHEN ZHI LING
 * Date: 2021/11/23
 * Description:
 */
class MongoDbSource(SQLContext: SQLContext,
                    options: Map[String, String],
                    schemaOption: Option[StructType]) extends Source with Logging {


  lazy val client: MongoClient = MongoDbUtils.createConnect(options)
  val database: String = options("database")
  val collectionName: String = options("collection")
  var currentOffset: Map[String, Long] = Map[String, Long](collectionName -> 0)


  override def schema: StructType = schemaOption.get


  override def getOffset: Option[Offset] = {
    val latest: Map[String, Long] = getLatestOffset
    Option(MongoDbSourceOffset(latest))
  }


  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    var offset: Long = 0
    if (start.isDefined) {
      offset = offset2Map(start.get)(collectionName)
    }
    val limit: Long = offset2Map(end)(collectionName) - offset
    val db: MongoDatabase = client.getDatabase(database)
    val collection: MongoCollection[Document] = db.getCollection(collectionName)
    //find new documents in the latest polling
    val documents: FindIterable[BsonDocument] = collection.find(classOf[BsonDocument]).limit(limit.toInt).skip(offset.toInt)
    val iterator: MongoCursor[BsonDocument] = documents.iterator()
    val rows: Seq[InternalRow] = MongoDbUtils.BsonDocumentToInternalRow(iterator, schema).toSeq
    currentOffset = offset2Map(end)
    val rdd: RDD[InternalRow] = SQLContext.sparkContext.parallelize(rows)
    SQLContext.internalCreateDataFrame(rdd, schema, isStreaming = true)
  }


  override def stop(): Unit = {
    client.close()
  }


  def getLatestOffset: Map[String, Long] = {
    val db: MongoDatabase = client.getDatabase(database)
    val count: Long = db.getCollection(collectionName).countDocuments()
    Map[String, Long](collectionName -> count)
  }


  def offset2Map(offset: Offset): Map[String, Long] = {
    implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)
    Serialization.read[Map[String, Long]](offset.json())
  }


  case class MongoDbSourceOffset(offset: Map[String, Long]) extends Offset {
    implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)

    override def json(): String = Serialization.write(offset)
  }
}