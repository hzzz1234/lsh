package com.dianping.search.offline.algorithm.minhash

import org.apache.spark.mllib.linalg.{SparseVector, Vectors,Vector}
import org.apache.spark.rdd.RDD

/**
  * Created by zhen.huaz on 2017/8/11.
  * @param origin_data 一般使用稀疏矩阵计算
  * @param p    超过向量最大维度的素数
  * @param numRows  稀疏签名矩阵的行数
  * @param numBands band数(用于拆分稀疏签名矩阵为多个局部相似度量)
  * @param minClusterSize 最小聚合数(一般为2)
  */
class MinHashLSH (origin_data : RDD[(String,Vector)], p : Int, numRows : Int, numBands : Int, minClusterSize : Int) extends Serializable {

  /** run MinhashLSH using the constructor parameters */
  def run(): MinHashLSHModel = {

    //创建一个minhash的模型
    val model = new MinHashLSHModel(p, numRows)

    val data = origin_data.map(line => (line._1,line._2.toSparse)).cache
    // 计算签名矩阵
    // - 对向量hash numrows次
    // - 将hash后的值定位到对应的band中,后面会根据band号进行分组,构建局部签名的桶
    // output ((vector idx, band#), minhash)
    val signatures = data.flatMap(v => model.hashFunctions.flatMap(h => List(((v._1, h._2 % numBands), h._1.minhash(v._2)))))

    // 对同band下的数据进行签名
    // output ((band+minlist)->bandhashvalue,key)
    val mid = signatures.groupByKey().map(x => (transforMD5(x._1._2, x._2), x._1._1)).cache()

    // output (key,hashlist)
    model.vector_hashlist = data.join(mid.map(x => x.swap).groupByKey().map(x => (x._1, x._2.toList))).map(row => (row._1,row._2._2)).cache()

    // 组织所有在同一个band且minhashlist的值一样的数据合并到一起
    // output (bandhashvalue, vectorid list)
    model.bands = mid.groupByKey().cache()

    //找到所有在聚合点数大于2点聚合id
    //(vector id, cluster id)
    model.vector_cluster = model.bands.filter(x => x._2.size >= minClusterSize) //获取大于2的
      .map(x => x._2.toList.sorted) //大于2的所有id排序
      .distinct()
      .zipWithIndex() //为每个向量组独立一个(vectorid list,cluster id)
      .map(x => x._1.map(y => (y, x._2))) //生成List((vectorid,clusterid),...)
      .flatMap(x => x.grouped(1)) //拆成独立的一个List(vectorid,clusterid),List(vectorid,clusterid),...
      .map(x => x(0)).cache() // (vectorid,clusterid)

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