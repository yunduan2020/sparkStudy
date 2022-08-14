package com.tom.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._

object Spark06_SparkSQL_Test1 {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "huxili")    // huxili为自己的hadoop用户名称

    // TODO 创建SparkSQL的运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()    // 不是导入包， 是把变量中的内容拿过来

    spark.sql("use sparkdata")

    spark.sql(
      """
        |select
        |    *
        |from (
        |    select
        |        *,
        |        rank() over( partition by area order by clickCnt desc ) as ranks
        |    from (
        |        select
        |           area,
        |           product_name,
        |           count(*) as clickCnt
        |        from (
        |            select
        |               a.*,
        |               p.product_name,
        |               c.area,
        |               c.city_name
        |            from user_visit_action a
        |            join product_info p on a.click_product_id = p.product_id
        |            join city_info c on a.city_id = c.city_id
        |            where a.click_product_id > -1
        |        ) t1 group by area, product_name
        |    ) t2
        |) t3 where ranks <= 3
      """.stripMargin).show()

    // TODO 关闭环境
    spark.close()
  }
}