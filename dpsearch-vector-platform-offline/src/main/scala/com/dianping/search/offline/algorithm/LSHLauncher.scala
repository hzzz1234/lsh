package com.dianping.search.offline.algorithm

import java.nio.file.FileSystem

import com.dianping.search.offline.algorithm.cosinelsh.CosineLSH
import com.dianping.search.offline.algorithm.e2lsh.E2LSH
import com.dianping.search.offline.algorithm.minhash.MinHashLSH
import com.dianping.search.offline.utils.PrimeUtils
import org.apache.hadoop.fs.Path
import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.{SparseVector, Vectors,Vector}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext

/**
  * Created by zhen.huaz on 2017/8/9.
  * 传入格式为 密集型(id,vector) 稀疏型(id,n,seq:values)
  * vector格式可以为2种类:
  */
object LSHLauncher {
  def main(args: Array[String]) {

    if(args.length != 12 && args.length != 11){

      System.err.println("Usage:LSHLauncher" +
        "<appname> [inputtype=1{1:file,2:sql}] <inputpath|inputsql> <outputpath> <row_spilter> <vector_spilter> <vectortype:{1:DenseVector,2:SparseVector}> \n" +
        "\t\t\t[lshtype=1{1:minhashlsh,2:cosinelsh,3:e2lsh}] <numbands> <num_in_a_band> <maxid> <m> \n" +
        "\t\t\t[lshtype=2{1:minhashlsh,2:cosinelsh,3:e2lsh}] <numbands> <num_in_a_band> <dimension> \n" +
        "\t\t\t[lshtype=3{1:minhashlsh,2:cosinelsh,3:e2lsh}] <numbands> <num_in_a_band> <distance> <tablesize>");
      System.exit(-1)

    }
    val APPNAME = args(0)
    val INPUTTYPE : Int = args(1).toInt
    val INPUT = args(2)
    val OUTPUT = args(3)
    val ROWSPILTER = args(4)
    val VECTORSPILTER = args(5)
    val VECTORTYPE = args(6).toInt

    var origin_data : RDD[(String,Vector)] = null
    val SPARKCONF = new SparkConf().setAppName(APPNAME)
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

    val LSHTYPE = args(7).toInt
    val NUMBANDS = args(8).toInt
    val NUM_IN_A_BAND = args(9).toInt
    val n = NUMBANDS * NUM_IN_A_BAND

    val hadoopConf = SPARKCONTEXT.hadoopConfiguration
    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    if(hdfs.exists(new Path(OUTPUT))){
      //为防止误删，禁止递归删除
      hdfs.delete(new Path(OUTPUT),true)
    }

    //算法类型
    if(LSHTYPE == 1){
      val maxid = args(10).toInt
      val m = args(11).toInt

      val lsh = new MinHashLSH(origin_data = origin_data, p = PrimeUtils.primes(maxid), numRows = n, numBands = NUMBANDS, minClusterSize = 2)
      val model = lsh.run

      model.vector_hashlist.map(line => line._1 + "\t" + line._2.mkString(" ")).saveAsTextFile(OUTPUT + "/vector_hashlist")
      model.scores.map(line => line._1 + "\t" + line._2).saveAsTextFile(OUTPUT+"/scores")
      SPARKCONTEXT.parallelize(model.hashFunctions).map(line => line.swap._1.toString()+"\t"+line.swap._2).saveAsTextFile(OUTPUT+"/hash")
      model.cluster_vector.groupByKey().map(line => line._1+"\t"+line._2.toList.mkString(" ")).saveAsTextFile(OUTPUT + "/cluster_vectorlist")
    } else if(LSHTYPE == 2){
      val dimension = args(10).toInt
      val lsh = new CosineLSH(origin_data = origin_data, dimension = dimension, numRows = n, numBands = NUMBANDS, minClusterSize = 2)
      val model = lsh.run

      model.vector_hashlist.map(line => line._1 + "\t" + line._2.mkString(" ")).saveAsTextFile(OUTPUT + "/vector_hashlist")
      model.scores.map(line => line._1 + "\t" + line._2).saveAsTextFile(OUTPUT+"/scores")
      SPARKCONTEXT.parallelize(model.hashVectors).map(line => line.swap._1.toString()+"\t"+line.swap._2).saveAsTextFile(OUTPUT+"/hash")
      model.cluster_vector.groupByKey().map(line => line._1+"\t"+line._2.toList.mkString(" ")).saveAsTextFile(OUTPUT + "/cluster_vectorlist")
    } else if(LSHTYPE == 3){
      val distance = args(10).toInt
      val tablesize = args(11).toInt

      val lsh = new E2LSH(origin_data = origin_data, distance = distance, numRows = n, numBands = NUMBANDS,ts = tablesize, minClusterSize = 2)
      val model = lsh.run
      model.vector_hashlist.map(line => line._1 + "\t" + line._2.mkString(" ")).saveAsTextFile(OUTPUT + "/vector_hashlist")
      model.scores.map(line => line._1 + "\t" + line._2).saveAsTextFile(OUTPUT+"/scores")
      SPARKCONTEXT.parallelize(model.hashFunctions).map(line => line.swap._1.toString()+"\t"+line.swap._2).saveAsTextFile(OUTPUT+"/hash")
      model.cluster_vector.groupByKey().map(line => line._1+"\t"+line._2.toList.mkString(" ")).saveAsTextFile(OUTPUT + "/cluster_vectorlist")
    } else {
      System.err.println("Usage:LSHLauncher" +
        "<appname> [inputtype=1{1:file,2:sql}] <inputpath|inputsql> <outputpath> <row_spilter> <vector_spilter> <vectortype:{1:DenseVector,2:SparseVector}> \n" +
        "\t\t\t[lshtype=1{1:minhashlsh,2:cosinelsh,3:e2lsh}] <numbands> <num_in_a_band> <maxid> <m> \n" +
        "\t\t\t[lshtype=2{1:minhashlsh,2:cosinelsh,3:e2lsh}] <numbands> <num_in_a_band> <dimension> \n" +
        "\t\t\t[lshtype=3{1:minhashlsh,2:cosinelsh,3:e2lsh}] <numbands> <num_in_a_band> <distance> <tablesize>");
      System.exit(-1)
    }






//    val inputtype : Int = args(0).toInt
//    var origin_data : RDD[(String,SparseVector)] = null
//    val sparkConf = new SparkConf().setAppName("aa").setMaster("local")
//    val sparkContext = new SparkContext(sparkConf)
//    var ornum : Int = 0
//    var andnum : Int = 0
//    var vectortype : Int = 0
//    var maxid : Int = 0
//    var lshtype : Int = 0
//    var tablesize : Int = 0
//    var output : String = null
//    if(inputtype == 1 && args.length == 13){
//
//      val appname : String = args(1)
//      val input : String = args(2)
//      output = args(3)
//      val hashout : String = args(4)
//      lshtype = args(5).toInt
//      val rowspilter : String = args(6)
//      val vectorspilter : String = args(7)
//      ornum = Integer.parseInt(args(8))
//      andnum = Integer.parseInt(args(9))
//      vectortype = Integer.parseInt(args(10))
//      maxid = args(11).toInt
//      tablesize = args(12).toInt
//      if(vectortype == 1) {
//
//        origin_data = sparkContext.textFile(input).map(line => (line.split(rowspilter)(0)
//          , Vectors.dense(for (ele <- line.split(rowspilter)(1).split(vectorspilter)) yield ele.toDouble).toSparse))
//      }
//      else{
//        origin_data = sparkContext.textFile(input).map(line => (line.split(rowspilter)(0),Vectors.sparse(line.split(rowspilter)(1).toInt,
//          for(ele <- line.split(rowspilter)(2).split(vectorspilter)) yield ele.split(":")(0).toInt,
//          for(ele <- line.split(rowspilter)(2).split(vectorspilter)) yield ele.split(":")(1).toDouble).toSparse))
//      }
//
//    } else if(inputtype == 2 && args.length == 12) {
//
//      val appname : String = args(0)
//      val input : String = args(2)
//      output = args(3)
//      val hashout : String = args(4)
//      lshtype = args(5).toInt
//      val spilter : String = args(6)
//      ornum = Integer.parseInt(args(7))
//      andnum = Integer.parseInt(args(8))
//      vectortype = Integer.parseInt(args(9))
//      maxid = args(10).toInt
//      tablesize = args(11).toInt
//      val hiveContext = new HiveContext(sparkContext)
//
//      if(vectortype == 1)
//        origin_data = hiveContext.sql(input).rdd.map(line => (line.getAs[String]("id")
//          ,Vectors.dense(for(ele <- line.getAs[String]("vector").split(" ")) yield ele.toDouble).toSparse))
//      else
//        origin_data = hiveContext.sql(input).rdd.map(line => (line.getAs[String]("id"),Vectors.sparse(line.getAs[Int]("n"),
//          for(ele <- line.getAs[String]("vector").split(" ")) yield ele.split(":")(0).toInt,
//          for(ele <- line.getAs[String]("vector").split(" ")) yield ele.split(":")(1).toDouble).toSparse))
//    } else {
//      System.err.println("Usage:LSHLauncher" +
//        "1.<appname> [inputtype=1] <inputpath> <outputpath> <hashpath> <row_spilter> <vector_splitor> <vectortype:{1:DenseVector,2:SparseVector}> \n" +
//        "2.<appname> [inputtype=2] <inputsql> <outputpath> <hashpath> <row_spilter> <vector_splitor> <vectortype:{1:DenseVector,2:SparseVector}> \n" +
//        "\t\t\t[lshtype=1{1:minhashlsh,2:cosinelsh,3:e2lsh}] <numbands> <num_in_a_band> <maxid> <m> \n" +
//        "\t\t\t[lshtype=2{1:minhashlsh,2:cosinelsh,3:e2lsh}] <numbands> <num_in_a_band> <dimension> \n" +
//        "\t\t\t[lshtype=3{1:minhashlsh,2:cosinelsh,3:e2lsh}] <numbands> <num_in_a_band> <distance> <tablesize>");
//      sSystem.exit(-1)
//    }
//    val n = ornum * andnum
//    val hadoopConf = sparkContext.hadoopConfiguration
//    val hdfs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
//    if(hdfs.exists(new Path(output))){
//      //为防止误删，禁止递归删除
//      hdfs.delete(new Path(output),true)
//    }
//
//    if(lshtype == 1){
//      val lsh = new MinHashLSH(data = origin_data, p = PrimeUtils.primes(maxid), numRows = n, numBands = ornum, minClusterSize = 2)
//      val model = lsh.run
//      model.vector_hashlist.map(line => line._1+"\t"+line._2._1+"\t"+line._2._2).saveAsTextFile(output)
////      println("samples: " + origin_data.count())
////      println("clusters: " + model.clusters.count())
////      for ( ele <- model.cluster_vector.collect())
////        println("cluter_vector:" + ele)
////      for ( ele <- model.vector_hashlist.collect())
////        println("result:" + ele._1 +","+ele._2._2)
////      for ( hashtab <- model.hashFunctions){
////        println("hashtab:" +hashtab)
////      }
////      for ( ele <- model.scores.collect())
////        println("r:" + ele)
//    } else if (lshtype == 2){
//      val origin_dense = origin_data.map(line => (line._1,line._2.toDense))
//      val lsh = new CosineLSH(data = origin_dense, p = maxid, numRows = n, numBands = ornum, minClusterSize = 2)
//      val model = lsh.run
//      model.vector_hashlist.map(line => line._1+"\t"+line._2._1+"\t"+line._2._2).saveAsTextFile(output)
////      println("samples: " + origin_data.count())
////      println("clusters: " + model.clusters.count())
////      for ( ele <- model.cluster_vector.collect())
////        println("cluter_vector:" + ele)
////      for ( ele <- model.vector_hashlist.collect())
////        println("result:" + ele)
////      for ( hashVector <- model.hashVectors){
////        println("hashVector:" +hashVector)
////      }
////      for ( ele <- model.scores.collect())
////        println("r:" + ele)
//
//    } else if (lshtype == 3) {
//      val origin_dense = origin_data.map(line => (line._1,line._2.toDense))
//      val lsh = new E2LSH(data = origin_dense, w = maxid, numRows = n, numBands = ornum,ts = tablesize, minClusterSize = 2)
//      val model = lsh.run
//      model.vector_hashlist.map(line => line._1+"\t"+line._2._1+"\t"+line._2._2).saveAsTextFile(output)
////      println("samples: " + origin_data.count())
////      println("clusters: " + model.clusters.count())
////      for ( ele <- model.cluster_vector.collect())
////        println("cluter_vector:" + ele)
////      for ( ele <- model.vector_hashlist.collect())
////        println("result:" + ele._1 +","+ele._2._2)
////      for ( hashVector <- model.hashFunctions){
////        println("hashVector:" +hashVector)
////      }
////      for ( ele <- model.scores.collect())
////        println("r:" + ele)
//
//    }


  }

}
