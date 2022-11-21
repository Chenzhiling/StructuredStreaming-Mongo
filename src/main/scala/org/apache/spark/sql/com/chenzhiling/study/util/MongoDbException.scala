package org.apache.spark.sql.com.chenzhiling.study.util

import com.mongodb.MongoException


/**
 * Author: CHEN ZHI LING
 * Date: 2021/11/23
 * Description:
 */
class MongoDbException(message: String, throwable: Throwable) extends MongoException(message, throwable) {


  def this(message: String) {
    this(message, null)
  }
}
