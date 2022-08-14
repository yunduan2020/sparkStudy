package com.tom.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._

object Spark06_SparkSQL_Test {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "huxili")    // huxili为自己的hadoop用户名称

    // TODO 创建SparkSQL的运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark: SparkSession = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()    // 不是导入包， 是把变量中的内容拿过来

    spark.sql("use sparkdata")

    // 准备数据
    spark.sql(
      """
        |CREATE TABLE `user_visit_action`(
        |`date` string,
        |`user_id` bigint,
        |`session_id` string,
        |`page_id` bigint,
        |`action_time` string,
        |`search_keyword` string,
        |`click_category_id` bigint,
        |`click_product_id` bigint,
        |`order_category_ids` string,
        |`order_product_ids` string,
        |`pay_category_ids` string,
        |`pay_product_ids` string,
        |`city_id` bigint)
        |row format delimited fields terminated by '_'
        |""".stripMargin)

    spark.sql(
      """
        |load data local inpath 'data/user_visit_action.txt' into table sparkdata.user_visit_action
        |""".stripMargin)

    spark.sql(
      """
        |CREATE TABLE `product_info`(
        |`product_id` bigint,
        |`product_name` string,
        |`extend_info` string)
        |row format delimited fields terminated by '\t'
        |
        |""".stripMargin)

    spark.sql(
      """
        | load data local inpath 'data/product_info.txt' into table sparkdata.product_info
        |""".stripMargin)

    spark.sql(
      """
        | CREATE TABLE `city_info`(
        |`city_id` bigint,
        |`city_name` string,
        |`area` string)
        |row format delimited fields terminated by '\t'
        |""".stripMargin)

    spark.sql(
      """
        | load data local inpath 'data/city_info.txt' into table sparkdata.city_info
        |""".stripMargin)

    spark.sql("""select * from city_info""").show

    // TODO 关闭环境
    spark.close()
  }
}