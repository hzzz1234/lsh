package com.dianping.search.offline.algorithm.cosinelsh

import java.lang.Math._

import com.dianping.search.offline.utils.MD5Utils
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.rdd.RDD

/**
  * Created by zhen.huaz on 2017/8/11.
  * support format : (vectorid,vector)
  */
/**
  *
  * @param origin_data  一般使用密集矩阵计算
  * @param dimension 向量维度
  * @param numRows  稀疏签名矩阵的行数
  * @param numBands band数(用于拆分稀疏签名矩阵为多个局部相似度量)
  * @param minClusterSize 最小聚合数(一般为2)
  */
class CosineLSH (origin_data : RDD[(String,Vector)], dimension : Int, numRows : Int, numBands : Int, minClusterSize : Int) extends Serializable {

  def rank(toList: List[(String, Int)]) = {
    val arr = new Array[String](toList.length);
    for( ele <- toList){
      arr(ele._2) = ele._1
    }
    arr.toList
  }

  /** run CosineLSH using the constructor parameters */
  def run() : CosineLSHModel = {

    //创建一个minhash的模型
    val model = new CosineLSHModel(dimension,numRows)

    val data = origin_data.map(line => (line._1,line._2.toDense)).cache
    // 计算签名矩阵
    // - 对向量hash numrows次
    // - 将hash后的值定位到对应的band中,后面会根据band号进行分组,构建局部签名的桶
    // output ((vector idx, band#), minhash)
    val signatures = data.flatMap(v => model.hashVectors.flatMap(h => List(((v._1, h._2 % numBands),h._1.cosinehash(v._2.toDense)))))

    // 对同band下的数据进行签名
    // output ((band+minlist)->(bandhashvalue,hashid),key)
    val mid = signatures.groupByKey().map(x => ((transforMD5(x._1._2,x._2),x._1._2), x._1._1)).cache()
    print(1)
    // output (key,hashlist)
    model.vector_hashlist = data.join(mid.map(x => x.swap).groupByKey().map(x => (x._1,rank(x._2.toList)))).map(row => (row._1,row._2._2)).cache()


    // 组织所有在同一个band且minhashlist的值一样的数据合并到一起
    // output ((bandhashvalue,hashid), vectorid list)
    model.bands = mid.groupByKey().cache()

    //找到所有在聚合点数大于2点聚合id
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

    //compute the cosine similarity of each cluster
    model.scores = model.clusters.map(row => (row._1, cosine(row._2.toList))).cache()

    model
  }

  def cosine(l: List[DenseVector]) : Double = {
    if(l.size < 1){
      return 0
    }
    var base = new Array[Double](l(0).size)
    for(ele <- l){
      sum(base,one_vector(ele))
    }

    var re = 0.0
    for(ele <- l){
      re += cosine(base,ele.toArray)
    }
    return re/l.size
  }

  def sum(a : Array[Double], b : Array[Double]) : Unit = {
    for( i <- 0 until a.size){
      a(i) = a(i) + b(i)
    }
  }
  def transforMD5(band : Int, iterable: Iterable[Int]): String ={
    val sb = new StringBuilder
    sb.append("[")
    for (it <- iterable) {
      sb.append(it+" ")
    }
    sb.append("]"+band)
    MD5Utils.md5Hash(sb.toString)
  }

  def transfor(band : Int, iterable: Iterable[Int]): String ={
    val sb = new StringBuilder
    sb.append("[")
    for (it <- iterable) {
      sb.append(it+" ")
    }
    sb.append("]"+band)
    sb.toString()
  }

  /** compute a single vector against an existing model */
  def compute(data : DenseVector, model : CosineLSHModel, minScore : Double) : RDD[(Long, Iterable[DenseVector])] = {
    model.clusters.map(x => (x._1, x._2++List(data))).filter(x => cosine(x._2.toList) >= minScore)
  }


  def cosine(a : Array[Double], b : Array[Double]) : Double = {
    if(a.size != b.size)
      return 0

    val dot_product = CosineHashVector.f2jBLAS.ddot(a.size,a,1,b,1)

    var normA = 0.0
    var normB = 0.0

    for( i <- 0 until a.size){
      if(a(i) != 0)
        normA += pow(a(i),2)

      if(b(i) != 0)
        normB += pow(b(i),2)
    }
    if (abs(normA)<1e-12 || abs(normB)<1e-12 )
      return 0;
    else
      return dot_product/math.sqrt(normA*normB)
  }

  /** 计算余弦相似度 */
  def cosine(a : DenseVector, b : DenseVector) : Double = {
    if(a.size != b.size)
      return 0
    val al = a.toArray
    val bl = b.toArray

    val dot_product = CosineHashVector.f2jBLAS.ddot(a.size,al,1,bl,1)

    var normA = 0.0
    var normB = 0.0

    for( i <- 0 until al.size){
      if(al(i) != 0)
        normA += pow(al(i),2)

      if(bl(i) != 0)
        normB += pow(bl(i),2)
    }

    if (abs(normA)<1e-12 || abs(normB)<1e-12 )
      return 0;
    else
      return dot_product/math.sqrt(normA*normB)
  }

  def one_vector(denseVector: DenseVector) : Array[Double] = {
    var r : Double = 0.0
    for(ele <- denseVector.values){
      r += ele * ele
    }
    if(r == 0)
      return denseVector.toArray
    val rs = Math.sqrt(r)
    var array = new Array[Double](denseVector.size)
    for( i <- 0 until denseVector.size){
      array(i) = denseVector.values(i)/rs
    }
    return array
  }

}
object CosineLSH{
  def cosine(a : DenseVector, b : DenseVector) : Double = {
    if(a.size != b.size)
      return 0
    val al = a.toArray
    val bl = b.toArray

    val dot_product = CosineHashVector.f2jBLAS.ddot(a.size,al,1,bl,1)

    var normA = 0.0
    var normB = 0.0

    for( i <- 0 until al.size){
      if(al(i) != 0)
        normA += pow(al(i),2)

      if(bl(i) != 0)
        normB += pow(bl(i),2)
    }

    if (abs(normA)<1e-12 || abs(normB)<1e-12 )
      return 0
    else
      return dot_product/math.sqrt(normA*normB)
  }

  def cosine(a : SparseVector, b : SparseVector) : Double = {
    val al = a.indices.toList
    val bl = b.indices.toList
    val l = al.union(bl).distinct
    var normA = 0.0
    var normB = 0.0
    var dot_product = 0.0
    val mapa = al.zip(a.values).toMap
    val mapb = bl.zip(b.values).toMap
    for(ele <- l){
      if(mapa.contains(ele) && mapb.contains(ele))
        dot_product += mapa.get(ele).get * mapb.get(ele).get

      if(mapa.contains(ele))
        normA += mapa.get(ele).get * mapa.get(ele).get

      if(mapb.contains(ele))
        normB += mapb.get(ele).get * mapb.get(ele).get
    }
    if (abs(normA)<1e-12 || abs(normB)<1e-12 )
      return 0;
    else
      return dot_product/math.sqrt(normA*normB)
  }
  def main(args: Array[String]) {
    println(    java.security.MessageDigest.getInstance("MD5").digest("adasdad".getBytes()).map(0xFF & _).map{"%02x".format(_)}.foldLeft(""){_+_})
    var a = Vectors.dense(Array(0.109442,0.39711,-0.182506,-0.334593,0.142774,0.237548,0.144811,0.782098,0.117924,0.168493,0.394658,-0.651512,-0.022314,0.246978,0.348534,0.345376,0.193348,-0.389754,-0.400227,0.097554,-0.407854,0.103744,-0.192567,0.236788,0.622972,-0.208338,-0.138787,0.295188,0.001933,0.201015,0.229646,-0.244834,0.518769,0.106359,-0.535816,-0.325545,0.395249,-0.514061,0.356312,-0.076938,0.703132,-0.607886,0.578341,-0.32574,-0.009738,0.706147,0.181072,-0.362883,0.055639,0.35991,0.590128,-0.055171,0.026439,-0.437734,-0.897892,0.130453,0.175267,-0.268129,0.340775,0.410248,-0.159216,-0.41879,-0.642714,0.071523,-0.568666,0.167865,0.532296,0.566135,-0.085136,-0.378162,-0.2844,-0.346932,-0.212987,0.04628,0.144059,0.314765,0.149584,-0.220441,0.697546,-0.027084,-0.43149,-0.263176,-0.184975,0.864755,0.295295,-0.342938,0.482071,0.431766,-0.035297,-0.353048,-0.077118,-0.382908,-0.002285,0.050558,0.437975,-0.065827,0.201389,0.666843,-0.315252,-0.770526,0.45535,-0.307302,0.069836,-0.362375,-0.036896,0.340933,0.091748,0.815805,-0.240115,-0.675604,-0.123134,0.33118,-0.350563,-0.027438,-0.596042,0.110696,-0.181513,0.173135,-0.628858,-0.557339,-0.354974,-0.165661,0.316757,0.064202,0.581215,0.46147,0.467016,0.314489,0.201434,-0.530994,-0.147977,-0.500575,0.055398,-0.550633,0.135079,0.644058,-0.334384,0.112417,0.127728,0.394493,0.334183,-0.282932,-0.248282,0.643657,-0.443138,-0.389805,0.273187,-0.110243,0.016793,0.115597,-0.541338,0.34005,-0.174256,-0.237505,0.519312,-0.10317,0.060825,0.492002,-0.087699,0.463067,0.003337,0.61521,0.672137,0.800635,0.458303,-0.618893,0.093453,0.472236,0.443193,0.114127,-0.602967,0.56005,0.174605,-0.356815,0.339684,-0.614975,0.354296,-0.094163,0.202019,-0.806419,-0.177146,-0.102466,0.177555,0.187637,-0.279256,-0.124775,-0.208501,0.242578,-0.028381,-0.767256,-0.152809,0.260068,0.45062,-0.022387,-0.271663,-0.521697,-0.735728,0.253614,-0.35307,-0.455566,0.111061,-0.295875,-0.085088,-0.084265,0.176575,-0.279112,-0.028766,-0.522314,-0.398874,-0.168992,0.454333,0.208779,-0.615228,0.355005,-0.199721,-0.430812,0.697851,0.026044,-0.655478,0.430617,-0.619605,-0.323443,-0.490243,0.514961,-0.403185,-0.297718,0.154154,0.729982,-0.242211,-0.011413,-0.399256,-0.037413,-0.298852,0.428803,-0.345943,-0.65657,-0.437846,0.177738,0.377243,-0.618813,-0.381639,-0.415126,0.009926,-0.085329,-0.537803,0.015928,-0.451391,0.771754,-0.29298,-0.1695,0.887428,0.322254,0.046367,-0.2928,0.312806,0.730047,-0.719202,0.071936,0.386191,-0.153034,-0.375728,-0.117442,0.645228,-0.165963,-0.146707,-0.573579,0.699664,-0.097671,-0.375235,0.228448,0.385524,0.218776,-0.044667,0.27348,0.204842,-0.030357,0.19777,-0.71894,-0.22267,0.30781,-0.349168,0.439846,0.231484,0.350731,-0.236642,-0.012656,0.356787,-0.300007,-0.092294,-0.546038,-0.265652,-0.658037,0.307653,0.915621,0.383458,0.189673,-0.220702,-0.322108,-0.513943,-0.329939)).toSparse
    var b = Vectors.dense(Array(1.2432785563301496,-5.48311567867246,-2.06181001155418,8.50390295784339,-30.825219557817327,27.114497615057882,-5.6620225037835095,-7.17891127416635,21.77386993107671,-21.757134368873043,2.7861735741643203,34.19477572039198,53.62317313962631,20.849376130318092,-22.989873929102075,37.71615464586654,12.11340239279204,5.32742356658439,2.8730451167748488,-20.126740200985008,-13.945264081253962,2.0161648524741613,-0.21478332639580028,-3.089003568519589,-5.471094198823989,-30.95481336495388,-34.35194564474766,-19.044253204531188,5.772835076295951,7.44038850051751,-0.8713744200191103,-2.6093558493144795,-15.87202442252829,2.55250734209433,-8.042961717516086,-20.000502871512428,0.9516438950338392,-13.917964478557751,20.65475075143339,3.962079754561389,-16.54843107754223,-8.143115161690408,6.9771938951624595,-24.428310018359724,-10.93407318898211,23.20831911413616,-7.5520269127464505,-16.015792146058796,12.217906208179198,6.7479592743065595,-8.7298201778609,2.8215521776075994,5.4967869963810685,-22.4975300996317,-14.404966760153638,-18.97029423478939,25.78967075604887,-16.15592706905677,-25.496784652039178,-23.307198824371202,-21.43450875449321,-1.3541242064332102,26.805338558009375,6.578737346412959,-5.430166797639979,10.31160359008347,23.068928369077966,0.5624119775409917,31.129855043292004,8.621777921724828,-8.80059380490767,38.117629550188795,-25.205088928552044,9.29508500459429,15.93711401364886,20.810637004922523,0.93353459929245,-8.859785994215038,-2.440431688120661,19.002009878799132,1.333129107959719,-18.542670483507536,-7.5444283234150715,-0.5077481813112289,10.227692351697371,3.429938833991891,-0.3803633782742807,-6.568761575128371,6.40868618510504,5.7538235780791585,-24.6022564667472,8.894786281371909,-12.039431476462571,-24.752297315061526,-5.437732957482099,15.026560726250827,0.8095127067009923,-10.05997913568447,-19.106762222207223,-31.01827975954829)).toSparse
    println(cosine(a,b))
    println(cosine(a.toDense,b.toDense))
    println(MD5Utils.md5Hash("[0 0 1 1 1 1 0 1 1 0 0 0 1 1 0 ]0"))
  }
}