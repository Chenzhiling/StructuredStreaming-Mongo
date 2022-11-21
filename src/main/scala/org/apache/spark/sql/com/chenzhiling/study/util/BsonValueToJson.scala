package org.apache.spark.sql.com.chenzhiling.study.util

import org.bson.codecs.{BsonValueCodec, EncoderContext}
import org.bson.internal.Base64
import org.bson.json._
import org.bson.{BsonBinary, BsonRegularExpression, BsonValue}

import java.io.StringWriter
import java.lang


/**
 * Author: CHEN ZHI LING
 * Date: 2021/11/24
 * Description:
 */
private[sql] object BsonValueToJson {

  val codec = new BsonValueCodec()

  def apply(element: BsonValue): String = {
    val stringWriter: StringWriter = new StringWriter
    val jsonWriter = new JsonWriter(stringWriter, JsonWriterSettings.builder().outputMode(JsonMode.EXTENDED).indent(false)
      .dateTimeConverter((value: lang.Long, writer: StrictJsonWriter) => {
        writer.writeStartObject()
        writer.writeNumber("$date", value.toLong.toString)
        writer.writeEndObject()
      })
      .int32Converter((value: Integer, writer: StrictJsonWriter) => {
        writer.writeNumber(value.toString)
      })
      .regularExpressionConverter((value: BsonRegularExpression, writer: StrictJsonWriter) => {
        writer.writeStartObject()
        writer.writeString("$regex", value.getPattern)
        writer.writeString("$options", value.getOptions)
        writer.writeEndObject()
      })
      .binaryConverter((value: BsonBinary, writer: StrictJsonWriter) => {
        writer.writeStartObject()
        writer.writeString("$binary", Base64.encode(value.getData))
        writer.writeString("$type", f"${value.getType}%02X")
        writer.writeEndObject()
      })
      .build())

    jsonWriter.writeStartDocument()
    jsonWriter.writeName("k")
    codec.encode(jsonWriter, element, EncoderContext.builder.build)
    stringWriter.getBuffer.toString.split(":", 2)(1).trim
  }
}
