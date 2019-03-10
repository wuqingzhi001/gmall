package com.gmall.offline.app

import com.gmall.common.utils.PropertiesUtil
import com.gmall.offline.udf.CityToRatio
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * @aythor HeartisTiger
  *         2019-03-07 19:33
  */
object AreaCountApp {
  def main(args: Array[String]): Unit = {
    var pro = PropertiesUtil.load("config.properties")
    var sparkconf = new SparkConf().setAppName("demo03").setMaster("local[*]")
    var sparkSession = SparkSession.builder().config(sparkconf).enableHiveSupport().getOrCreate()
    sparkSession.sql("use gmall")
    sparkSession.udf.register("city_ratio",new CityToRatio())
  sparkSession.sql("select c.area,u.click_product_id,c.city_name from user_visit_action u join city_info c on c.city_id=u.city_id  where u.click_product_id >0").createOrReplaceTempView("t1")

    sparkSession.sql("select area,click_product_id,count(*) clickcount , city_ratio(city_name) city " +

      "from t1 group by area,click_product_id ").createOrReplaceTempView("t2")
    sparkSession.sql("select * ,rank() over(partition by area order by clickcount desc) rk from t2").createOrReplaceTempView("t3")
    sparkSession.sql("select area,product_name,clickcount,city from t3 ,product_info p where t3.rk <=3 and p.product_id = t3.click_product_id").write.format("jdbc")
      .option("url",pro.getProperty("jdbc.url"))
      .option("user",pro.getProperty("jdbc.user"))
      .option("password",pro.getProperty("jdbc.password"))
      .option("dbtable", "area_count_info").mode(SaveMode.Append).save()
}
}
