
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.{Vectors, Vector}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkContext, SparkConf}
import Profile.userProfile
import scala.collection.immutable
import scala.collection.mutable.ArrayBuffer
import tw.com.chttl.spark.mllib.util.NAStat

/**
 * Created by leorick on 2016/5/6.
 */
object Spatial {
  val appName = "woplus.spatial"

  case class spatialDetail(imei:String, date:String, hour:Int, x:Double, y:Double,
                           areaid:Int, clusterid:Int)

  def str2spatialDetail(line:String): spatialDetail = {
    val toks = line.split(""",""")
    spatialDetail(toks(0), toks(1), toks(2).toInt, toks(3).toDouble, toks(4).toDouble,
      toks(5).toInt, toks(6).toInt)
  }

  def spatialDetail2Str(obj:spatialDetail) = {
    Array(obj.imei, obj.date, obj.hour.toString, obj.x.toString, obj.y.toString, obj.areaid.toString, obj.clusterid.toString).mkString(",")
  }

  def row2spatialDetail(row:Row): spatialDetail = {
    spatialDetail(row.getString(0),row.getString(1),row.getInt(2),row.getDouble(3),row.getDouble(4),
      row.getInt(5),row.getInt(6))
  }

  def splitSrc(src:RDD[String], delimiter:String, header:String = ""): RDD[Array[String]] = {
    val tokens = src.map{_.split(delimiter)}
    header.size match {
      case 0 => tokens
      case _ => tokens.filter{ case toks => !toks(1).contains(header) } }
  }

  def splitSpatialSrc(srcSpatial:RDD[String]): RDD[Array[String]] = {
    // 逐一解析欄位
    srcSpatial.mapPartitions{ ite =>
      ite.map { line =>
        val ary = new ArrayBuffer[String]()
        var start = 0
        var end = 0
        var idx = 0
        var tok = ""
        while (start <= line.size) {
          idx = line.indexOf(",", start)
          end = if( idx < 0 || idx > line.size ) line.size else idx
          tok = line.substring(start, end)
          ary += (tok.size match {
            case 0 => "0"
            case _ => tok })
          start = end + 1
        }
        ary.toArray } }
  }

  case class spatialLog(imei:String, date:String, hour:Int, x:Double, y:Double)

  def spatialLog2Str(log:spatialLog) = {
    Array(log.imei,log.date,log.hour.toString,log.x.toString,log.y.toString).mkString(",")
  }

  def tok2Spatial(spatial:RDD[Array[String]]): RDD[spatialLog] = {
    spatial.flatMap{ ary =>
      (2 to 48 by 2).
        map{ idx =>
        spatialLog(ary(0), ary(1), (idx/2-1), ary(idx).toDouble, ary(idx+1).toDouble) }.
        filter{ log => log.x != 0 && log.y != 0 } }
  }

  def loadKModel(sc:SparkContext, path:String): KMeansModel = {
    val vecs = sc.textFile(path).
      map{ line => line.split(""",""") }.
      map{ toks => Vectors.dense( toks.map{_.toDouble} ) }.collect()
    new KMeansModel( vecs )
  }

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(sparkConf)
    sc.getConf.set("spark.kryoserializer.buffer.mb","128")
    val sqlContext = new SQLContext(sc)
    import sqlContext._
    // 讀取完整資料
    var path = "hdfs:/user/leoricklin_taoyuan4g/woplus/spatial/"
    /*
    var spatialToks = splitSrc(loadSrc(sc, path), """,""")
    NAStat.statsWithMissing(spatialToks.map{ tokens => Array(tokens.size)})
 Array(stats: (count: 5776605, mean: 42.687266, stdev: 9.450486, max: 50.000000, min: 4.000000), NaN: 0)
     */
    // 分隔GPS紀錄
    var spatialToks = splitSpatialSrc(Device.loadSrc(sc, path))
    NAStat.statsWithMissing(spatialToks.map{ tokens => Array(tokens.size)})
    /*
Array(stats: (count: 5776605, mean: 50.000000, stdev: 0.000000, max: 50.000000, min: 50.000000), NaN: 0)
     */
    // 將GPS紀錄轉成 detail record
    val spatiallogs = tok2Spatial(spatialToks).cache()
    spatiallogs.getStorageLevel.useMemory
    spatiallogs.take(10).map{spatialLog2Str(_)}.mkString("\n")
    /*
20160105,ff7cfb0e717cc3a48af443209168ef92,0,121.2839,31.34085001
20160105,ff7cfb0e717cc3a48af443209168ef92,1,121.2839,31.34085001
20160105,ff7cfb0e717cc3a48af443209168ef92,2,121.2839,31.34085001
20160105,ff7cfb0e717cc3a48af443209168ef92,3,121.2839,31.34085001
20160105,ff7cfb0e717cc3a48af443209168ef92,4,121.2839,31.34085001
20160105,ff7cfb0e717cc3a48af443209168ef92,5,121.2839,31.34085001
20160105,ff7cfb0e717cc3a48af443209168ef92,6,121.2839,31.34085001
20160105,ff7cfb0e717cc3a48af443209168ef92,7,121.2839,31.34085001
20160105,ff7cfb0e717cc3a48af443209168ef92,8,121.2839,31.34085001
20160105,ff7cfb0e717cc3a48af443209168ef92,9,121.2839,31.34085001
     */
    // 定義四大區域
    val areas = sc.
      broadcast( Seq(
      0->Seq(121.44149, 121.48956, 31.2354, 31.30025),  // 大宁地区
      1->Seq(121.49814, 121.55445, 31.23981, 31.28999), // 北外滩区域
      2->Seq(121.4245,  121.49934, 31.18446, 31.23482), // 卢湾地区
      3->Seq(121.50621, 121.55977, 31.19018, 31.23658) ).// 三林地区
      toMap ).
      value
    // 載入模型
    var kmodels: Map[Int, KMeansModel] = areas.
      map{ case (idx, area) =>
      path = f"hdfs:/user/leoricklin_taoyuan4g/woplus/spatial.kmean/${idx}%02d"
      (idx, loadKModel(sc, path)) }
    println( kmodels.
      map{ case (idx, model) =>
      idx + "="*50 + "\n" +
        model.clusterCenters.map{ vec => " "*2 + vec.toArray.mkString(",") }.mkString("\n") }.mkString("\n") )
    /*
0==================================================
  121.47904218021975,31.27436613439561
  121.4822382,31.242465427164177
  121.46295560847456,31.268907813050856
  121.47989070408164,31.292249250714285
  121.45070153015872,31.2451486634127
  121.45020977500002,31.289576749423084
  121.44969039230764,31.26387677423077
  121.46505258314606,31.250673503483142
  121.48237673983056,31.25877582330509
  121.47152513499998,31.238059283699982
1==================================================
  121.54497624999998,31.279527461136365
  121.54562910434782,31.24945867739131
  121.52273102857141,31.243289817346938
  121.50350558823527,31.285694119411765
  121.50478317391305,31.256429499130434
  121.50794794347829,31.274031308695655
  121.51631573500002,31.262953368499993
  121.50350845,31.242291090185176
  121.52527335294117,31.281408139411763
  121.53139298507465,31.264620886567158
2==================================================
  121.48775176810342,31.226870036293093
  121.43753732790697,31.226237231686042
  121.43246179481484,31.192586475629632
  121.45436309874212,31.22898911861635
  121.47196186949999,31.22731812925001
  121.48546462410714,31.203046577053577
  121.44844290000002,31.195363215287358
  121.46792063923073,31.20550328438461
  121.45229112400004,31.213080561359995
  121.43183557291667,31.21165184875
3==================================================
  121.52187648039215,31.231059664313722
  121.51504965714284,31.19749158371429
  121.55043973333335,31.209229098333328
  121.5122770906977,31.230297268604662
  121.52864429887637,31.225297347865173
  121.5136195622222,31.21369013822222
  121.54395201166665,31.226609231833336
  121.54822319090908,31.195514252272734
  121.53635720454548,31.212127111363635
  121.5247843088889,31.208801892
     */

    //
    spatiallogs.unpersist(true)
  }

  def reortA(sc:SparkContext, areas:Map[Int, Seq[Double]]) = {
    val sqlContext = new SQLContext(sc)
    import sqlContext._
    // 載入用戶位置紀錄
    val ite = areas.toIterator
    var path = f"hdfs:/user/leoricklin_taoyuan4g/woplus/spatial.detail/${ite.next()._1}%02d"
    var spatialDetails: RDD[spatialDetail] = Device.loadSrc(sc, path).map{ line => str2spatialDetail(line) }
    ite.foreach{ case (idx, area) =>
      path = f"hdfs:/user/leoricklin_taoyuan4g/woplus/spatial.detail/${idx}%02d"
      spatialDetails = spatialDetails.union( Device.loadSrc(sc, path).map{ line => str2spatialDetail(line) } ) }
    spatialDetails.count // 19722375
    spatialDetails.getStorageLevel.useMemory
    val spatialDetailTableName = "spatialdetail"
    spatialDetails.registerTempTable(spatialDetailTableName)
    var df = sqlContext.sql(f"select conv(s.imei, 16, 10), s.date, s.hour, s.x, s.y, s.areaid, s.clusterid from ${spatialDetailTableName} s")
    var result = df.limit(10).collect()
    println(result.map{ row => spatialDetail2Str(row2spatialDetail(row)) }.mkString("\n"))
    df = spatialDetails.where{'date === "20160101"}.limit(5)
    result = df.collect
    println(result.map{ row => spatialDetail2Str(row2spatialDetail(row)) }.mkString("\n"))
    /*
Array([bf3f04034044fd5c9d5f9445c3a3eda3,20160102,9,121.4837113,31.2601311,0,8])
     */
    // 載入用戶標籤紀錄
    path = "hdfs:/user/leoricklin_taoyuan4g/woplus/usertag.uniq"
    val userTags: RDD[userProfile] = Device.loadSrc(sc, path).map{ line => Profile.str2Userprofile(line)}.cache()
    userTags.getStorageLevel.useMemory
    val usertagTableName = "usertag"
    userTags.registerTempTable(usertagTableName)
    // join結果筆數不符預期
    df = sqlContext.sql("select s.imei, s.date, s.hour, s.x, s.y, s.areaid, s.clusterid," +
      f" u.imei, u.fa, u.fb, u.fc, u.fd, u.fe," +
      f" u.ff, u.fg, u.fh, u.fi, u.fj," +
      f" u.fk, u.fl, u.fm, u.fn, u.fo," +
      f" u.fp, u.fq, u.fr, u.fs, u.ft" +
      f" from ${spatialDetailTableName} s" +
      f" left outer join ${usertagTableName} u on s.imei = u.imei " +
      f" where s.date = '20160101' and s.hour = 10 and s.areaid = 0 and s.clusterid = 0" )
    df.count // = 2112
    //
    result = df.take(10)
    println( result.
      map{ row =>
      val spatial = spatialDetail2Str(spatialDetail(row.getString(0),row.getString(1),row.getInt(2),row.getDouble(3),row.getDouble(4),
        row.getInt(5),row.getInt(6)))
      val user = Profile.userprofile2Str(userProfile(row.getString(7),
        row.getDouble(8),row.getDouble(9),row.getDouble(10),row.getDouble(11),row.getDouble(12),
        row.getDouble(13),row.getDouble(14),row.getDouble(15),row.getDouble(16),row.getDouble(17),
        row.getDouble(18),row.getDouble(19),row.getDouble(20),row.getDouble(21),row.getDouble(22),
        row.getDouble(23),row.getDouble(24),row.getDouble(25),row.getDouble(26),row.getDouble(27)))
      f"[${spatial}][${user}]" }.
      mkString("\n") )
    /*
[823f42c137efff86f845ec3de1be86d8,20160101,10,121.48944,31.26972001,0,0][823f42c137efff86f845ec3de1be86d8,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0]
[6975deb6c8ef98149c54119d806310c4,20160101,10,121.4833,31.26765001,0,0][6975deb6c8ef98149c54119d806310c4,0.0,0.0,0.0,0.0,1.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0]
[4053a6e7bb61dd91204db21b5d9dd7c2,20160101,10,121.4775,31.26889001,0,0][4053a6e7bb61dd91204db21b5d9dd7c2,0.0,0.0,0.0,0.0,0.0,1.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0]
     */
    //
    //
    userTags.unpersist(true)
    spatialDetails.unpersist(true)

  }
}
