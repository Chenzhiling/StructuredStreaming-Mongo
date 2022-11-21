package org.apache.spark.sql.com.chenzhiling.study.util

import com.mongodb.MongoTimeoutException
import com.mongodb.client.{MongoClient, MongoClients, MongoCursor}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.NextIterator
import org.bson.BsonDocument



/**
 * Author: CHEN ZHI LING
 * Date: 2021/11/23
 * Description:
 */
object MongoDbUtils extends Logging{


  /**
   * connect to mongo
   * @param options params
   * @return
   */
  def createConnect(options: Map[String, String]): MongoClient = {
    val uri: String = options("uri")
    var mongoClient: MongoClient = null
    try {
      mongoClient = MongoClients.create(uri)
      mongoClient
    } catch {
      case _: MongoTimeoutException => throw new MongoDbException("connection failed")
    }
  }


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
}
