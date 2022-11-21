package org.apache.spark.sql.com.chenzhiling.study.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, DateTimeUtils}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.bson._

import java.sql.{Date, Timestamp}
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}


/**
 * Author: CHEN ZHI LING
 * Date: 2021/11/24
 * Description: type conversion function between Bson and spark-internalRow
 */
object MongoDbConvertor {


  /**
   * change document into an  InternalRow
   * @param bsonDocument document
   * @param schema schema
   * @return
   */
  def documentToInternalRow(bsonDocument: BsonDocument, schema: StructType): InternalRow = {
    val values: Array[(Any, StructField)] = schema.fields.map((field: StructField) =>
      if (bsonDocument.containsKey(field.name)) {
        (convertToDataType(bsonDocument.get(field.name), field.dataType), field)
      } else {
        (null, field)
      })
    new GenericInternalRow(values.map((_: (Any, StructField))._1))
  }


  /**
   * change type
   * @param element bson value
   * @param elementType bson type
   * @return
   */
  private def convertToDataType(element: BsonValue, elementType: DataType): Any = {
    (element.getBsonType, elementType) match {
      //scala.collection.immutable.Map => MapData
      case (BsonType.DOCUMENT, mapType: MapType) =>
        ArrayBasedMapData.apply(element.asDocument().asScala.map((kv: (String, BsonValue))
        => (UTF8String.fromString(kv._1), convertToDataType(kv._2, mapType.valueType))).toMap)
      // scala.collection.immutable.Array => ArrayData
      case (BsonType.ARRAY, arrayType: ArrayType) => ArrayData.toArrayData(
        element.asArray().getValues.asScala.map(convertToDataType(_: BsonValue, arrayType.elementType)).toArray)
      case (BsonType.BINARY, BinaryType) => element.asBinary().getData
      case (BsonType.BOOLEAN, BooleanType) => element.asBoolean().getValue
      // use spark DateTimeUtils function to convert
      case (BsonType.DATE_TIME, DateType) => DateTimeUtils.fromJavaDate(new Date(element.asDateTime().getValue))
      case (BsonType.DATE_TIME, TimestampType) => DateTimeUtils.fromJavaTimestamp(new Timestamp(element.asDateTime().getValue))
      case (BsonType.NULL, NullType) => null
      case (isBsonNumber(), DoubleType) => toDouble(element)
      case (isBsonNumber(), FloatType) => toFloat(element)
      case (isBsonNumber(), IntegerType) => toInt(element)
      case (isBsonNumber(), ShortType) => toShort(element)
      case (isBsonNumber(), ByteType) => toByte(element)
      case (isBsonNumber(), LongType) => toLong(element)
      case (isBsonNumber(), _) if elementType.typeName.startsWith("decimal") => toDecimal(element)
      case (notNull(), schema: StructType) => castToStructType(element, schema)
      //String => UTF8String
      case (_, StringType) => bsonValueToString(element)
      case _ =>
        if (element.isNull) {
          null
        } else {
          throw new MongoDbException(s"Cannot cast ${element.getBsonType} into a $elementType (value: $element)")
        }
    }
  }


  private def bsonValueToString(element: BsonValue): UTF8String = {
    //use UTF8String type instead of String
    element.getBsonType match {
      case BsonType.STRING    => UTF8String.fromString(element.asString().getValue)
      case BsonType.OBJECT_ID => UTF8String.fromString(element.asObjectId().getValue.toHexString)
      case BsonType.INT64     => UTF8String.fromString(element.asInt64().getValue.toString)
      case BsonType.INT32     => UTF8String.fromString(element.asInt32().getValue.toString)
      case BsonType.DOUBLE    => UTF8String.fromString(element.asDouble().getValue.toString)
      case BsonType.NULL      => null
      case _                  => UTF8String.fromString(BsonValueToJson(element))
    }
  }


  private def castToStructType(element: BsonValue, elementType: StructType): Any = {
    (element.getBsonType, elementType) match {
      case (BsonType.BINARY, BsonInternalRowCompatibility.Binary()) =>
        BsonInternalRowCompatibility.Binary(element.asInstanceOf[BsonBinary], elementType)
      case (BsonType.DOCUMENT, _) =>
        documentToInternalRow(element.asInstanceOf[BsonDocument], elementType)
      case (BsonType.DB_POINTER, BsonInternalRowCompatibility.DbPointer()) => BsonInternalRowCompatibility.DbPointer(element.asInstanceOf[BsonDbPointer], elementType)
      case (BsonType.JAVASCRIPT, BsonInternalRowCompatibility.JavaScript()) =>
        BsonInternalRowCompatibility.JavaScript(element.asInstanceOf[BsonJavaScript], elementType)
      case (BsonType.JAVASCRIPT_WITH_SCOPE, BsonInternalRowCompatibility.JavaScriptWithScope()) =>
        BsonInternalRowCompatibility.JavaScriptWithScope(element.asInstanceOf[BsonJavaScriptWithScope], elementType)
      case (BsonType.MIN_KEY, BsonInternalRowCompatibility.MinKey()) =>
        BsonInternalRowCompatibility.MinKey(element.asInstanceOf[BsonMinKey], elementType)
      case (BsonType.MAX_KEY, BsonInternalRowCompatibility.MaxKey()) =>
        BsonInternalRowCompatibility.MaxKey(element.asInstanceOf[BsonMaxKey], elementType)
      case (BsonType.OBJECT_ID, BsonInternalRowCompatibility.ObjectId()) =>
        BsonInternalRowCompatibility.ObjectId(element.asInstanceOf[BsonObjectId], elementType)
      case (BsonType.REGULAR_EXPRESSION, BsonInternalRowCompatibility.RegularExpression()) =>
        BsonInternalRowCompatibility.RegularExpression(element.asInstanceOf[BsonRegularExpression], elementType)
      case (BsonType.SYMBOL, BsonInternalRowCompatibility.Symbol()) =>
        BsonInternalRowCompatibility.Symbol(element.asInstanceOf[BsonSymbol], elementType)
      case (BsonType.TIMESTAMP, BsonInternalRowCompatibility.Timestamp()) =>
        BsonInternalRowCompatibility.Timestamp(element.asInstanceOf[BsonTimestamp], elementType)
      case (BsonType.UNDEFINED, BsonInternalRowCompatibility.Undefined()) =>
        BsonInternalRowCompatibility.Undefined(element.asInstanceOf[BsonUndefined], elementType)
      case _ => throw new MongoDbException(s"Cannot cast ${element.getBsonType} into a $elementType (value: $element)")
    }
  }


  private object isBsonNumber {
    val bsonNumberTypes = Set(BsonType.INT32, BsonType.INT64, BsonType.DOUBLE, BsonType.DECIMAL128)

    def unapply(x: BsonType): Boolean = bsonNumberTypes.contains(x)
  }


  private def toInt(bsonValue: BsonValue): Int = {
    bsonValue.getBsonType match {
      case BsonType.DECIMAL128 => bsonValue.asDecimal128().decimal128Value().bigDecimalValue().intValue()
      case BsonType.INT32 => bsonValue.asInt32().intValue()
      case BsonType.INT64 => bsonValue.asInt64().intValue()
      case BsonType.DOUBLE => bsonValue.asDouble().intValue()
      case _ => throw new MongoDbException(s"Cannot cast ${bsonValue.getBsonType} into a Int")
    }
  }


  private def toShort(bsonValue: BsonValue): Short = {
    Try(toInt(bsonValue).toShort) match {
      case Success(v) => v
      case Failure(_) => throw new MongoDbException(s"Cannot cast ${bsonValue.getBsonType} into a Short")
    }
  }


  private def toByte(bsonValue: BsonValue): Byte = {
    Try(toInt(bsonValue).toByte) match {
      case Success(v) => v
      case Failure(_) => throw new MongoDbException(s"Cannot cast ${bsonValue.getBsonType} into a Byte")
    }
  }


  private def toLong(bsonValue: BsonValue): Long = {
    bsonValue.getBsonType match {
      case BsonType.DECIMAL128 => bsonValue.asDecimal128().decimal128Value().bigDecimalValue().longValue()
      case BsonType.INT32 => bsonValue.asInt32().longValue()
      case BsonType.INT64 => bsonValue.asInt64().longValue()
      case BsonType.DOUBLE => bsonValue.asDouble().longValue()
      case _ => throw new MongoDbException(s"Cannot cast ${bsonValue.getBsonType} into a Long")
    }
  }


  private def toDouble(bsonValue: BsonValue): Double = {
    bsonValue.getBsonType match {
      case BsonType.DECIMAL128 => bsonValue.asDecimal128().decimal128Value().bigDecimalValue().doubleValue()
      case BsonType.INT32 => bsonValue.asInt32().doubleValue()
      case BsonType.INT64 => bsonValue.asInt64().doubleValue()
      case BsonType.DOUBLE => bsonValue.asDouble().doubleValue()
      case _ => throw new MongoDbException(s"Cannot cast ${bsonValue.getBsonType} into a Double")
    }
  }


  private def toFloat(bsonValue: BsonValue): Double = {
    Try(toDouble(bsonValue).toFloat) match {
      case Success(v) => v
      case Failure(_) => throw new MongoDbException(s"Cannot cast ${bsonValue.getBsonType} into a Float")
    }
  }


  private def toDecimal(bsonValue: BsonValue): BigDecimal = {
    bsonValue.getBsonType match {
      case BsonType.DECIMAL128 => bsonValue.asDecimal128().decimal128Value().bigDecimalValue()
      case BsonType.INT32 => BigDecimal(bsonValue.asInt32().intValue())
      case BsonType.INT64 => BigDecimal(bsonValue.asInt64().longValue())
      case BsonType.DOUBLE => BigDecimal(bsonValue.asDouble().doubleValue())
      case _ => throw new MongoDbException(s"Cannot cast ${bsonValue.getBsonType} into a BigDecimal")
    }
  }


  private object notNull {
    def unapply(x: BsonType): Boolean = x != BsonType.NULL
  }
}
