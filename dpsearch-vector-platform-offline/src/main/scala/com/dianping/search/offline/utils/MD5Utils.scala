package com.dianping.search.offline.utils

/**
  * Created by zhen.huaz on 2017/10/18.
  */
object MD5Utils {
  /**
    *
    * @param text
    * @return
    */
  def md5Hash(text:String):String =
    java.security.MessageDigest.getInstance("MD5").digest(text.getBytes()).map(0xFF & _).map{"%02x".format(_)}.foldLeft(""){_+_}.substring(8,24)

}
