package com.dianping.search

import com.dianping.search.offline.algorithm.cosinelsh.CosineHashVector
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Hello world!
 *
 */
object HelloWorld {
  def main(args: Array[String]): Unit = {
    val SPARKCONF = new SparkConf()
      .setAppName("aa").setMaster("local")
    val SPARKCONTEXT = new SparkContext(SPARKCONF)
    val hashtext = SPARKCONTEXT.textFile("incrhash").collect();
    var hash = List[(CosineHashVector, Int)]()
    for( line <- hashtext){
      val splits = line.split("\t");

      val ele: Array[String] = splits(1).substring(1, splits(1).length - 1).trim.split(" ")
      val ds: Array[Double] = new Array[Double](ele.length)
      var i: Int = 0
      while (i < ele.length) {
        {
          ds(i) = ele(i).toDouble
        }
        ({
          i += 1; i - 1
        })
      }

      val vector = new CosineHashVector(300,ds)
      hash.::=(vector,splits(0).toInt)
    }
    hash = hash.reverse
    for ( ele <- hash ) {
      println(ele)
    }
  }
}
