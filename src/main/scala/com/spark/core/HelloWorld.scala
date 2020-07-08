package com.spark.core
import java.util.Properties

import org.apache.log4j.Logger
import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scala.language.implicitConversions

import scala.io.Source
object HelloWorld {
  @transient lazy val logger = Logger.getLogger(getClass.getName)

  def getSparkConf:SparkConf={
    val conf = new SparkConf()
    val props = new Properties()
    props.load(Source.fromFile("C:/Users/user/IdeaProjects/SecondApp/spark.conf").bufferedReader())
    props.forEach((k,v)=> conf.set(k.toString,v.toString))
    conf
  }

  def main(args: Array[String]): Unit = {
      logger.info("Creating spark session ")
      val spark = SparkSession.builder().config(getSparkConf).getOrCreate()
      println(spark.conf.getAll.toList)
      logger.info("Created spark session")
      val lst = List(1,2,3,4,5)
    logger.info("Creating rdd")
      val rdd = spark.sparkContext.parallelize(lst)

    logger.info(rdd.collect().mkString(","))





      spark.stop()
  }
}
