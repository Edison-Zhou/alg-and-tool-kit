package cn.moretv.doraemon.common.util

/**
  *
  * @author liu.qiang 
  * @since 2018/7/30 15:01
  *
  * 该类包含了将节目sid转化为id和将32位的userid转化为长整型id
  */
object IdTransfer {
  val stringkey: String = "a1b2cd3e4f5ghi6jklm7nop8qrstu9vwx0yz"
  val numberKey: Array[Int] = Array(1, 86, 21, 62, 59, 17)

  def getKey(): (Seq[String],scala.collection.mutable.Map[String,Int]) = {
    var numString = Seq[String]()
    val stringNum = scala.collection.mutable.Map[String,Int]()
    (0 to 99).foreach(i=>{
      val strTmp: String = String.valueOf(stringkey.charAt(i % 33)) + String.valueOf(stringkey.charAt((i % 33) + 1 + (i / 33)))
      numString = numString :+ strTmp
      stringNum.put(strTmp, i)
    }
    )
    (numString,stringNum)
  }

  /**
    *
    * @param code 视频sid
    * @return 视频的id
    */
  def codeToId(code: String): Int = {
    var id = -1
    if (code.length == 12) {
      val temp = getKey()
      val stringNum = temp._2
      var stringId: String = ""

      (0 to 5).foreach(j => {
        val a: String = String.valueOf(code.charAt(2 * j))
        val b: String = String.valueOf(code.charAt(2 * j + 1))
        var n: Int = stringNum.get(a + b).get
        n = n - numberKey(j)
        if (n < 0) {
          n = n + 100
        }
        var ns: String = String.valueOf(n)
        if (ns.length < 2) {
          ns = "0" + ns
        }
        stringId += ns
      })
      id = Integer.valueOf(stringId.substring(1, String.valueOf(stringId.charAt(0)).toInt + 2))
    }
    id
  }

  /**
    *
    * @param s userid
    * @return  userid转化为的长整型数字
    */
  def userId2Long(s:String) = {
    var hash:Long = 0
    for(i<- 0 until s.length){
      hash = 47*hash + s.charAt(i).toLong
    }

    hash = math.abs(hash)

    if(hash<1000000000){
      hash = hash + 1000000000*(hash+"").charAt(0).toInt
    }
    hash
  }

  def main(args: Array[String]): Unit = {
    val s1 = "3frua29wce3e" //3fru1b9w4g8s, 3fru1b9whj9x
    println(codeToId(s1))
    val s2 = "3frua29wfh1b" //3fru1b9w4g8s, 3fru1b9whj9x
    println(codeToId(s2))

  }

}
