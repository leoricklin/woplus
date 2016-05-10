import org.apache.spark.mllib.feature.Normalizer
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import scala.collection.immutable.IndexedSeq

/**
 * Created by leorick on 2016/5/6.
 */
object Profile {
  val appName = "woplus.profile"

  case class userProfile(imei:String,fa:Double,fb:Double,fc:Double,fd:Double,fe:Double,
                         ff:Double,fg:Double,fh:Double,fi:Double,fj:Double,
                         fk:Double,fl:Double,fm:Double,fn:Double,fo:Double,
                         fp:Double,fq:Double,fr:Double,fs:Double,ft:Double )


  def str2Userprofile(line:String): userProfile = {
    val ary = line.split(""",""")
    userProfile(ary(0),
      ary(1).toDouble,ary(2).toDouble,ary(3).toDouble,ary(4).toDouble,ary(5).toDouble,
      ary(6).toDouble,ary(7).toDouble,ary(8).toDouble,ary(9).toDouble,ary(10).toDouble,
      ary(11).toDouble,ary(12).toDouble,ary(13).toDouble,ary(14).toDouble,ary(15).toDouble,
      ary(16).toDouble,ary(17).toDouble,ary(18).toDouble,ary(19).toDouble,ary(20).toDouble)
  }

  def vector2Userprofile(imei:String, vec:Vector): userProfile = {
    userProfile(imei, vec(0), vec(1), vec(2), vec(3), vec(4),
      vec(5), vec(6), vec(7), vec(8), vec(9),
      vec(10), vec(11), vec(12), vec(13), vec(14),
      vec(15), vec(16), vec(17), vec(18), vec(19))
  }

  def userprofile2Vector(obj:userProfile): Vector = {
    Vectors.dense(Array(
      obj.fa,obj.fb,obj.fc,obj.fd,obj.fe,
      obj.ff,obj.fg,obj.fh,obj.fi,obj.fj,
      obj.fk,obj.fl,obj.fm,obj.fn,obj.fo,
      obj.fp,obj.fq,obj.fr,obj.fs,obj.ft))
  }

  def userprofile2Str(obj:userProfile): String = {
    obj.imei + "," + Array(obj.fa,obj.fb,obj.fc,obj.fd,obj.fe,
      obj.ff,obj.fg,obj.fh,obj.fi,obj.fj,
      obj.fk,obj.fl,obj.fm,obj.fn,obj.fo,
      obj.fp,obj.fq,obj.fr,obj.fs,obj.ft).mkString(",")
  }

  def row2Userprofile(row:Row): userProfile = {
    userProfile(row.getString(0),
      row.getDouble(1),row.getDouble(2),row.getDouble(3),row.getDouble(4),row.getDouble(5),
      row.getDouble(6),row.getDouble(7),row.getDouble(8),row.getDouble(9),row.getDouble(10),
      row.getDouble(11),row.getDouble(12),row.getDouble(13),row.getDouble(14),row.getDouble(15),
      row.getDouble(16),row.getDouble(17),row.getDouble(18),row.getDouble(19),row.getDouble(20))
  }

  def splitProfileSrc(src:RDD[String]): RDD[String] = {
    src.map{_.split(",")}.
      filter{ toks => !toks(0).contains("IMEI") }.  // remove header
      map{ toks => toks.map{ tok => tok.replaceAll(""""""", "") } }. // remove double quote
      map{ toks =>
      val buf = toks.toBuffer
      buf.remove(6,2)         // 是否有跨省行为,是否有出国行为
      buf.remove(buf.size-2)  // 访问其他类网站的次数
      buf.map{ tok => if (tok.size == 0) "0" else tok }.toArray.mkString(",") }
  }

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    import sqlContext._
    // 載入 & 分隔資料
    var path = "hdfs:/user/leoricklin_taoyuan4g/woplus/userprofile"
    val userProfiles = splitProfileSrc(Device.loadSrc(sc, path)).map{str2Userprofile(_)}
    userProfiles.getStorageLevel.useMemory
    /*
    userProfiles.count = 2391166
    userProfiles.map(_.imei).distinct().count // = 642548
    userProfiles.unpersist(true)
     */
    // 單一用戶彙整
    val userProfileTable = "userprofile"
    userProfiles.registerTempTable(userProfileTable)
    /*
    var df = sqlContext.sql(f"select * from ${userProfileTable}")
    var result = df.limit(10).collect()
    println(result.map{ row => userProfile2Str(row2Userprofile(row)) }.mkString("\n"))
     */
    val userProfileUniq = sqlContext.
      sql("select u.imei, sum(u.fa), sum(u.fb), sum(u.fc), sum(u.fd), sum(u.fe)," +
      f" sum(u.ff), sum(u.fg), sum(u.fh), sum(u.fi), sum(u.fj)," +
      f" sum(u.fk), sum(u.fl), sum(u.fm), sum(u.fn), sum(u.fo)," +
      f" sum(u.fp), sum(u.fq), sum(u.fr), sum(u.fs), sum(u.ft)" +
      f" from ${userProfileTable} u" +
      f" group by u.imei")
    /*
    userProfileUniq.count  = 642548
     */
    // 屬性正規化
    val norm = new Normalizer()
    val userProfileUniqNormal = userProfileUniq.
      map{ row =>
      val user = row2Userprofile(row)
      ( user.imei, norm.transform(userprofile2Vector(user)) ) }.cache()
    userProfileUniqNormal.getStorageLevel.useMemory
    /*
    userProfileNormal.count = 642548
     */
    // 建立次數分配
    val featureHists: IndexedSeq[(Int, (Array[Double], Array[Long]))] = (0 to 19).
      map{ idx =>
      (idx, userProfileUniqNormal.map{ case (imei, features) => features(idx) }.histogram(5)) }
    // 建立貼標們檻
    val tagThresholds: Map[Int, Double] = sc.broadcast(
      featureHists.
        map{ case (idx, (cent , height)) => (idx, cent(4)) }.toMap ).value
    /*
 Map(
   0 -> 0.8, 5 -> 0.8, 10 -> 0.8, 14 -> 0.8, 1 -> 0.6565151134646567, 6 -> 0.8
 , 9 -> 0.8, 13 -> 0.7993615974550592, 2 -> 0.2926931692721623, 17 -> 0.8, 12 -> 0.8
 , 7 -> 0.7985710799058339, 3 -> 0.8, 18 -> 0.8, 16 -> 0.8, 11 -> 0.8
 , 8 -> 0.7997977442885773, 19 -> 0.8, 4 -> 0.8, 15 -> 0.8)
     */
    // 轉換用戶屬性標籤
    var userTags: RDD[userProfile] = userProfileUniqNormal.
      mapPartitions{ ite =>
      ite.map{ case (imei, features) =>
        val tags: Vector = Vectors.dense( tagThresholds.
          map{ case (idx, threshold) => (idx, if (features(idx) > threshold) 1 else 0) }.
          toArray.
          sortBy{ case (idx, flag) => idx }.
          map{ case (idx, flag) => flag.toDouble })
        vector2Userprofile(imei, tags) } }.
      cache()
    userTags.getStorageLevel.useMemory
    userTags.count // = 642548
    println(userTags.take(10).map{ up => userprofile2Str(up) }.mkString("\n"))
    userProfileUniqNormal.unpersist(true)
    // 儲存
    path = "hdfs:/user/leoricklin_taoyuan4g/woplus/usertag.uniq"
    userTags.saveAsTextFile(path)

    /*
        var path = "hdfs:/user/leoricklin_taoyuan4g/woplus/usertag/usertag.csv"
        val userTags: RDD[userProfile] = Device.loadSrc(sc, path).map{ line => str2Userprofile(line)}.cache()
        userTags.getStorageLevel.useMemory
        userTags.count // = 2,391,166
        // IMEI存在重複
        userTags.map{_.imei}.distinct().count() // = 642,548
        val usertagTableName = "usertag"
        userTags.registerTempTable(usertagTableName)
        var df = sqlContext.sql("select * from usertag")
        var result = df.limit(10).collect()
        println( result.map{ row =>
          f"[${userProfile2Str(row2Userprofile(row))}][${Device.bytes2hex(row.getString(0).getBytes)}}]" }.mkString("\n"))
        /*
    2ab41bf4da442c8b2f3306c28d15d919,0.0,0.0,0.0,0.0,1.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0
         */
        df = sqlContext.sql("select imei,sum(fa),sum(fb),sum(fc),sum(fd),sum(fe)," +
          "sum(ff),sum(fg),sum(fh),sum(fi),sum(fj)," +
          "sum(fk),sum(fl),sum(fm),sum(fn),sum(fo)," +
          "sum(fp),sum(fq),sum(fr),sum(fs),sum(ft)" +
          " from usertag group by imei")
        df.count // = 642548
        //
        path = "hdfs:/user/leoricklin_taoyuan4g/woplus/usertag.uniq"
        df.map{ row => userProfile2Str(row2Userprofile(row)) }.saveAsTextFile(path)
        df.saveAsTextFile(path)
        userTags.unpersist(true)
    */
    // 載入用戶標籤紀錄
    path = "hdfs:/user/leoricklin_taoyuan4g/woplus/usertag.uniq"
    userTags = Device.loadSrc(sc, path).map{ line => str2Userprofile(line)}.cache()
    userTags.getStorageLevel.useMemory
    userTags.count // = 642548


  }
}
