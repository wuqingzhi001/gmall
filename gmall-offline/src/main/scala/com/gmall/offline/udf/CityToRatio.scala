package com.gmall.offline.udf

import java.text.DecimalFormat

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import scala.collection.immutable.HashMap
import scala.collection.mutable

/**
  * @aythor HeartisTiger
  *         2019-03-07 20:16
  */
class CityToRatio extends UserDefinedAggregateFunction{

  //定义输入 类型
  override def inputSchema: StructType = StructType(Array(StructField("city_name",StringType)))

  //定义存储类型 类型
  override def bufferSchema: StructType = StructType(Array(StructField("city_count",MapType(StringType,LongType)),StructField("total",LongType)))

  //定义输出类型 String
  override def dataType: DataType = StringType

  //验证 相同的输入会有相同的输出
  override def deterministic: Boolean = true

  //存储的初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = new HashMap[String,Long]
    buffer(1) = 0L
  }

  //更新，每到一条数据做一次更新
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {

    val citycount = buffer.getAs[HashMap[String,Long]](0)
    val totalcount = buffer.getAs[Long](1)
    val cityname = input.getString(0)
    buffer(0) = citycount +(cityname->(citycount.getOrElse(cityname,0L)+1L))
    buffer(1) = totalcount+1L
  }

  //合并 每个分区处理完成 汇总到driver时进行合并
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val map1 = buffer1.getAs[HashMap[String,Long]](0)
    val map2 = buffer2.getAs[HashMap[String,Long]](0)
    val count1 = buffer1.getAs[Long](1)
    val count2 = buffer1.getAs[Long](1)
  buffer1(0) =   map1.foldLeft(map2){
      case (map2,(cityname,count))=>
          map2 + (cityname->(map2.getOrElse(cityname,0L)+count))
    }
    buffer1(1) = count1+count2

  }

  //把存储中的数据展示出来
  override def evaluate(buffer: Row): Any = {
    val map = buffer.getAs[HashMap[String,Long]](0)
    val count = buffer.getLong(1)
    val ratioMap1 = new mutable.HashMap[String,Double]
    val list = map.map {
      case (cityname, count1) =>
        var raito = new DecimalFormat("#.00").format(cityname -> count1 * 1.0 / count).toDouble
        CityRatioInfo(cityname, raito)
    }.toList

    list.sortWith((x,y)=>
      if(x.ratio>y.ratio){
        true
      }else {
        false
      }
    ).take(2)



  }
}
case class CityRatioInfo(cityname:String,ratio:Double)
