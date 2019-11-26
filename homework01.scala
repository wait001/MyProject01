package com.qf.gp1010

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object homework01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TestTransformation").setMaster("local").set("spark.testing.memory","512000000")
    val sc = new SparkContext(conf)
    //1.创建RDD
    val rdd: RDD[Int] = sc.parallelize(List(5,6,4,7,3,9,2,10,8))
    val rdd2: RDD[Int] = rdd.map(_ * 2) //行动算子 collect 将RDD中数据转换成一个集合进行存储
    println(rdd2.collect().toBuffer)
    sc.stop()
  }
}
