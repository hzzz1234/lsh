package com.dianping.search.offline.algorithm.minhash

import org.apache.spark.mllib.linalg.{SparseVector, Vectors}
import org.apache.spark.rdd.RDD

/**
  * Created by zhen.huaz on 2017/8/11.
  * support format : (vectorid,vector)
  */
class MinHashLSH (data : RDD[(String,SparseVector)], p : Int, numRows : Int, numBands : Int, minClusterSize : Int) extends Serializable {

  /** run LSH using the constructor parameters */
  def run(): MinHashLSHModel = {

    //创建一个minhash的模型
    val model = new MinHashLSHModel(p, numRows)


    //compute signatures from matrix
    // - hash each vector <numRows> times
    // - position hashes into bands. we'll later group these signature bins and has them as well
    //this gives us ((vector idx, band#), minhash)
    val signatures = data.flatMap(v => model.hashFunctions.flatMap(h => List(((v._1, h._2 % numBands), h._1.minhash(v._2))))).cache()

    val mid = signatures.groupByKey().map(x => (transforMD5(x._1._2, x._2), x._1._1)).cache()
    model.vector_hashlist = data.join(mid.map(x => x.swap).groupByKey().map(x => (x._1, x._2.toList))).map(row => (row._1, (row._2._1, row._2._2)))

    //reorganize data for shuffle
    //this gives us ((band#, hash of minhash list), vector id)
    //groupByKey gives us items that hash together in the same band
    model.bands = mid.groupByKey().cache()

    //we only want groups of size >= <minClusterSize>
    //(vector id, cluster id)
    model.vector_cluster = model.bands.filter(x => x._2.size >= minClusterSize) //获取大于2的
      .map(x => x._2.toList.sorted) //大于2的所有id排序
      .distinct()
      .zipWithIndex() //为每个向量组独立一个id
      .map(x => x._1.map(y => (y, x._2)))
      .flatMap(x => x.grouped(1))
      .map(x => x(0)).cache()

    //(cluster id, vector id)
    model.cluster_vector = model.vector_cluster.map(x => x.swap).cache()

    //(cluster id, List(vector))
    model.clusters = data.join(model.vector_cluster).map(x => (x._2._2, x._2._1)).groupByKey().cache()

    //compute the jaccard similarity of each cluster
    model.scores = model.clusters.map(row => (row._1, (jaccard(row._2.toList), row._2.size))).cache()

    model
  }

  def transforMD5(band: Int, iterable: Iterable[Int]): String = {
    val sb = new StringBuilder
    sb.append("[")
    for (it <- iterable) {
      sb.append(it + " ")
    }
    sb.append("]" + band)
    md5Hash(sb.toString)
  }

  /**
    *
    * @param text
    * @return
    */
  def md5Hash(text: String): String =
    java.security.MessageDigest.getInstance("MD5").digest(text.getBytes()).map(0xFF & _).map {
      "%02x".format(_)
    }.foldLeft("") {
      _ + _
    }


  /** compute a single vector against an existing model */
  def compute(data: SparseVector, model: MinHashLSHModel, minScore: Double): RDD[(Long, Iterable[SparseVector])] = {
    model.clusters.map(x => (x._1, x._2 ++ List(data))).filter(x => jaccard(x._2.toList) >= minScore)
  }

  /** compute jaccard between two vectors */
  def jaccard(a: SparseVector, b: SparseVector): Double = {
    val al = a.indices.toList
    val bl = b.indices.toList
    al.intersect(bl).size / al.union(bl).size.doubleValue
  }

  /** compute jaccard similarity over a list of vectors */
  def jaccard(l: List[SparseVector]): Double = {
    l.foldLeft(l(0).indices.toList)((a1, b1) => a1.intersect(b1.indices.toList.asInstanceOf[List[Nothing]])).size /
      l.foldLeft(List())((a1, b1) => a1.union(b1.indices.toList.asInstanceOf[List[Nothing]])).distinct.size.doubleValue
  }

}

object MinHashLSH{
  def jaccard(a: SparseVector, b: SparseVector): Double = {
    val al = a.indices.toList
    val bl = b.indices.toList
    al.intersect(bl).size / al.union(bl).distinct.size.doubleValue
  }
  def main(args: Array[String]) {
    var a = Vectors.dense(Array(1.0,1.0,0.0,1.0,0.0,0.0,0.0,0.0,1.0,1.0,1.0,1.0,1.0,1.0,0.0,1.0,0.0,0.0,1.0,0.0,0.0,1.0,0.0,1.0,1.0,1.0,0.0,0.0,1.0,0.0,1.0,1.0,0.0,1.0,0.0,0.0,1.0,0.0,1.0,1.0,1.0,1.0,0.0,1.0,0.0,1.0,0.0,0.0,0.0,0.0,0.0,1.0,1.0,1.0,1.0,0.0,0.0,1.0,0.0,0.0,0.0,0.0,0.0,1.0,1.0,0.0,0.0,1.0,0.0,0.0,1.0,1.0,1.0,1.0,0.0,0.0,1.0,0.0,0.0,0.0,0.0,1.0,1.0,0.0,0.0,1.0,1.0,1.0,1.0,0.0,0.0,1.0,1.0,0.0,0.0,1.0,1.0,0.0,1.0,1.0)).toSparse
    var b = Vectors.dense(Array(1.0,1.0,1.0,1.0,0.0,0.0,1.0,0.0,0.0,0.0,1.0,0.0,1.0,1.0,1.0,0.0,0.0,0.0,0.0,0.0,1.0,1.0,1.0,0.0,1.0,1.0,0.0,0.0,1.0,0.0,1.0,0.0,1.0,0.0,0.0,0.0,0.0,1.0,1.0,0.0,1.0,1.0,0.0,1.0,1.0,1.0,0.0,1.0,1.0,0.0,1.0,0.0,0.0,1.0,0.0,1.0,1.0,0.0,0.0,1.0,0.0,1.0,0.0,1.0,1.0,0.0,1.0,0.0,1.0,0.0,1.0,1.0,0.0,0.0,0.0,1.0,0.0,0.0,1.0,1.0,0.0,0.0,0.0,0.0,1.0,1.0,0.0,0.0,1.0,0.0,0.0,0.0,0.0,0.0,1.0,1.0,1.0,1.0,0.0,1.0)).toSparse
    println(jaccard(a,b))
  }
}