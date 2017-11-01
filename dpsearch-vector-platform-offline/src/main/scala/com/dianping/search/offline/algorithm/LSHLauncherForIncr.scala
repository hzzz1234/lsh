package com.dianping.search.offline.algorithm

import java.io.BufferedReader

import breeze.io.TextReader.InputStreamReader
import com.dianping.search.offline.algorithm.cosinelsh.{CosineHashVector, CosineLSH, CosineLSHForIncr}
import com.dianping.search.offline.algorithm.e2lsh.E2LSH
import com.dianping.search.offline.algorithm.minhash.MinHashLSH
import com.dianping.search.offline.utils.{PrimeUtils, VectorUtils}
import org.apache.hadoop.fs.Path
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhen.huaz on 2017/8/9.
  * 传入格式为 密集型(id,vector) 稀疏型(id,n,seq:values)
  * vector格式可以为2种类:
  */
object LSHLauncherForIncr {
  def main(args: Array[String]) {

    if(args.length != 13 && args.length != 12){

      System.err.println("Usage:LSHLauncher" +
        "<appname> <type> [inputtype=1{1:file,2:sql}] <inputpath|inputsql> <outputpath> <row_spilter> <vector_spilter> <vectortype:{1:DenseVector,2:SparseVector}> \n" +
//        "\t\t\t[lshtype=1{1:minhashlsh,2:cosinelsh,3:e2lsh}] <numbands> <num_in_a_band> <maxid> <m> \n" +
        "\t\t\t[lshtype=2{1:minhashlsh,2:cosinelsh,3:e2lsh}] <numbands> <num_in_a_band> <dimension> <hashpath>\n" +
//        "\t\t\t[lshtype=3{1:minhashlsh,2:cosinelsh,3:e2lsh}] <numbands> <num_in_a_band> <distance> <tablesize>"
         "");
      System.exit(-1)

    }
    val APPNAME = args(0)
    val output_type = args(1)
    val INPUTTYPE : Int = args(2).toInt
    val INPUT = args(3)
    val OUTPUT = args(4)
    val ROWSPILTER = args(5)
    val VECTORSPILTER = args(6)
    val VECTORTYPE = args(7).toInt

    var origin_data : RDD[(String,Vector)] = null
    val SPARKCONF = new SparkConf()
//      .setAppName(APPNAME).setMaster("local")
    val SPARKCONTEXT = new SparkContext(SPARKCONF)

    //输入数据初始化
    if(INPUTTYPE == 1){
      if(VECTORTYPE == 1){

        origin_data = SPARKCONTEXT.textFile(INPUT).map(line => (line.split(ROWSPILTER)(0)
          , Vectors.dense(for (ele <- line.split(ROWSPILTER)(1).split(VECTORSPILTER)) yield ele.toDouble)))
      } else {
        origin_data = SPARKCONTEXT.textFile(INPUT).map(line => (line.split(ROWSPILTER)(0),Vectors.sparse(line.split(ROWSPILTER)(1).toInt,
                  for(ele <- line.split(ROWSPILTER)(2).split(VECTORSPILTER)) yield ele.split(":")(0).toInt,
                  for(ele <- line.split(ROWSPILTER)(2).split(VECTORSPILTER)) yield ele.split(":")(1).toDouble)))
      }
    } else {
      val HIVECONTEXT = new HiveContext(SPARKCONTEXT)
      if(VECTORTYPE == 1){
        origin_data = HIVECONTEXT.sql(INPUT).rdd.map(line => (line.getAs[String]("id")
                    ,Vectors.dense(for(ele <- line.getAs[String]("vector").split(" ")) yield ele.toDouble)))
      } else {
        origin_data = HIVECONTEXT.sql(INPUT).rdd.map(line => (line.getAs[String]("id"),Vectors.sparse(line.getAs[Int]("n"),
                  for(ele <- line.getAs[String]("vector").split(" ")) yield ele.split(":")(0).toInt,
                  for(ele <- line.getAs[String]("vector").split(" ")) yield ele.split(":")(1).toDouble)))
      }
    }

    val LSHTYPE = args(8).toInt
    val NUMBANDS = args(9).toInt
    val NUM_IN_A_BAND = args(10).toInt
    val n = NUMBANDS * NUM_IN_A_BAND

    val hadoopConf = SPARKCONTEXT.hadoopConfiguration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    if(hdfs.exists(new Path(OUTPUT))){
      //为防止误删，禁止递归删除
      hdfs.delete(new Path(OUTPUT),true)
    }



    //算法类型
    if(LSHTYPE == 2){
      val dimension = args(11).toInt
      //解析过程
      val hashpath = args(12)
      val hashtext = SPARKCONTEXT.textFile(hashpath).collect();
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

        val vector = new CosineHashVector(dimension,ds)
        hash.::=(vector,splits(0).toInt)
      }
      hash = hash.reverse


      val lsh = new CosineLSHForIncr(origin_data = origin_data, dimension = dimension, numRows = n, numBands = NUMBANDS, minClusterSize = 2,hash)
      val model = lsh.run

      model.scores.map(line => line._1 + "\t" + line._2).saveAsTextFile(OUTPUT+"/scores")
      SPARKCONTEXT.parallelize(model.hashVectors).map(line => line.swap._1.toString()+"\t"+line.swap._2+"\t"+NUMBANDS+"\t"+NUM_IN_A_BAND).saveAsTextFile(OUTPUT+"/hash")
      model.cluster_vector.groupByKey().map(line => line._1+"\t"+line._2.toList.mkString(" ")).saveAsTextFile(OUTPUT + "/cluster_vectorlist")

      origin_data.join(model.vector_hashlist).map(row => row._1+"\t"+output_type+"\t"+VectorUtils.dotT(row._2._1.toArray)+"\t"+row._2._1.toArray.toList.mkString(" ")+"\t"+row._2._2.mkString("\t")).saveAsTextFile(OUTPUT + "/vectorid_vector_hashlist")
    } else { //使用时需修改
      System.err.println("Usage:LSHLauncher" +
        "<appname> <type> [inputtype=1{1:file,2:sql}] <inputpath|inputsql> <outputpath> <row_spilter> <vector_spilter> <vectortype:{1:DenseVector,2:SparseVector}> \n" +
        "\t\t\t[lshtype=1{1:minhashlsh,2:cosinelsh,3:e2lsh}] <numbands> <num_in_a_band> <maxid> <m> \n" +
        "\t\t\t[lshtype=2{1:minhashlsh,2:cosinelsh,3:e2lsh}] <numbands> <num_in_a_band> <dimension> \n" +
        "\t\t\t[lshtype=3{1:minhashlsh,2:cosinelsh,3:e2lsh}] <numbands> <num_in_a_band> <distance> <tablesize>");
      System.exit(-1)
    }
  }

}
