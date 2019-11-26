package com.qf.gp1010

import org.apache.spark.{SparkConf, SparkContext}

object homework {
  def main(args: Array[String]): Unit = {
    val spark=new SparkContext(new SparkConf().setAppName("homework").setMaster("local").set("spark.testing.memory","512000000"))
    val rdd1=spark.parallelize(Array(("aa",3),("cc",6),("bb",2),("dd",1)))
    println("11111")
    rdd1.foreach(println)
    val rdd2 = rdd1.map(add=>{
      (add._2,add._1)
    })
      val value = rdd2.sortByKey(true)
    val rdd3 = value.map(add=>{
      (add._2,add._1)
    })
    rdd3.foreach(print)
    spark.stop()
  }
}
