package com.gmall.offline.acc

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
  * @aythor HeartisTiger
  *         2019-03-07 8:51
  */
class CategoryAcc extends AccumulatorV2[String,mutable.HashMap[String,Long]]{
  var map = new mutable.HashMap[String,Long]()
  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = {
    val other = new CategoryAcc()
    other.map ++= this.map
    other
  }

  override def reset(): Unit = {map.clear()}

  override def add(key: String): Unit = {
   map(key) =  map.getOrElse(key,0L)+1L
  }

  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {
    //两个map的常规合并操作
      val otherMap = other.value
     map =  map.foldLeft(otherMap){case (otherMap,(key,count))=>
      otherMap(key)= otherMap.getOrElse(key,0L)+count
      otherMap
    }

  }

  override def value: mutable.HashMap[String, Long] = map
}
