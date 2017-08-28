package com.dianping.search

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhen.huaz on 2017/8/9.
  */
object WordCount {
  def main(args: Array[String]) {
//    if (args.length < 1) {
//      System.err.println("Usage: <file>")
//      System.exit(1)
//    }
    val conf = new SparkConf()
    conf.setMaster("local").setAppName("wordcnt")
    val sc = new SparkContext(conf)
    val line = sc.textFile("ceshi.txt")

    val counts = line.flatMap(line => List(line.split(" "),1))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    for (ele <- counts.collect()){
      println(ele)
    }

  }
}
