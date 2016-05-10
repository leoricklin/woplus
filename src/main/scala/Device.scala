
import _root_.tw.com.chttl.spark.mllib.util.NAStat
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.{RandomForest}
import org.apache.spark.mllib.tree.model.{RandomForestModel, DecisionTreeModel, Node}
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql._
import tw.com.chttl.spark.mllib.util.NAStat
import scala.collection.immutable.IndexedSeq

/**
 * Created by leorick on 2016/5/6.
 */
object Device {
  val appName = "woplus.device"

  def bytes2hex(bytes: Array[Byte], sep: Option[String] = None): String = {
    sep match {
      case None => bytes.toSeq.map("%02x".format(_)).mkString
      case _ => bytes.toSeq.map("%02x".format(_)).mkString(sep.get)
    }
  }

  def loadSrc(sc:SparkContext, path:String): RDD[String] = {
    sc.textFile(path)
  }

  def splitSrc(src:RDD[String], delimiter:String, header:String = ""): RDD[Array[String]] = {
    val tokens = src.map{_.split(delimiter)}
    header.size match {
      case 0 => tokens
      case _ => tokens.filter{ case toks => !toks(1).contains(header) } }
  }

  case class Imsi(imsi:String)

  def tok2Imsi(tokens:RDD[String]): RDD[Imsi] = {
    tokens.map{ line => Imsi(line) }
  }

  def row2Imsi(row:Row): Imsi = {
    Imsi(row.getString(0))
  }

  def imsi2Str(imsi:Imsi): String = {
    imsi.imsi
  }

  // [0.month: int, 1.imsi: string, 2.net: string, 3.gender: string, 4.age: string,
  // 5.arpu: string, 6.dev_vendor: string, 7.dev_type: string, 8.bytes: string, 9.voice: int,
  // 10.sms: int]
  case class deviceLog(month:Int, imsi:String, net:String, gender:String, age:String,
                       arpu:String, dev_vendor:String, dev_type:String, bytes:String, voice:Int,
                       sms:Int)

  def tok2DeviceLog(tokens:RDD[Array[String]]): RDD[deviceLog] = {
    tokens.map{ ary => deviceLog(ary(0).replaceAll(""""""","").toInt, ary(1), ary(2), ary(3), ary(4),
      ary(5), ary(6), ary(7), ary(8), ary(9).replaceAll(""""""","").toInt, ary(10).replaceAll(""""""","").toInt) }
  }

  def row2DeviceLog(row:Row): deviceLog = {
    deviceLog(row.getInt(0), row.getString(1), row.getString(2), row.getString(3), row.getString(4),
      row.getString(5), row.getString(6), row.getString(7), row.getString(8), row.getInt(9),
      row.getInt(10))
  }

  def deviceLog2Str(dev:deviceLog): String = {
    Array(dev.month.toString, dev.imsi, dev.net, dev.gender, dev.age,
      dev.arpu, dev.dev_vendor, dev.dev_type, dev.bytes, dev.voice.toString,
      dev.sms.toString).mkString(",")
  }

  def deviceLog2Vector(dev:deviceLog, mapping:Map[Int, Map[Int, Int]]): (String, Vector) = {
    ( dev.imsi, Vectors.dense(
      mapping(2)(dev.net.hashCode).toDouble,    // 0,net
      mapping(3)(dev.gender.hashCode).toDouble, // 1,gender
      mapping(4)(dev.age.hashCode).toDouble,    // 2,age
      mapping(5)(dev.arpu.hashCode).toDouble,   // 3,arpu
      mapping(8)(dev.bytes.hashCode).toDouble,  // 4,bytes
      dev.voice.toDouble,                       // voice
      dev.sms.toDouble ) )                       // sms
  }

  def findDiffDev(sqlContext:SQLContext, deviceAll: IndexedSeq[(Int, RDD[deviceLog], String)],
                  imsiTagetTableName:String, deviceLogTableNamePref:String)
  : IndexedSeq[(Int, SchemaRDD)] = {
    deviceAll.
      flatMap{ case (idx, df, tablename) => idx match {
      case 1 | 9 =>
        None // 1月沒有前個月資料, (1~8) & (9~12) 月手機格式不同
      case _ => {
        val diff = sqlContext.sql(
          f"select a.imsi" +
            f", nx.dev_vendor as n_vendor, nx.dev_type as n_type" +
            f", bf.dev_vendor as b_vendor, bf.dev_type as b_type" +
            f" from ${imsiTagetTableName} a" + // 需要预测的IMSI
            f" left join ${deviceLogTableNamePref}${idx}%02d   nx on a.imsi = nx.imsi" +
            f" left join ${deviceLogTableNamePref}${idx-1}%02d bf on a.imsi = bf.imsi" +
            f" where nx.dev_vendor <> bf.dev_vendor and nx.dev_type <> bf.dev_type")
        Some(idx, diff) } } }
  }

  def prev1MDeviceLabel(sqlContext:SQLContext, diffDeviceImsis:IndexedSeq[(Int, SchemaRDD)],
                        imsiTagetTableName:String, deviceLogTableNamePref:String): SchemaRDD = {
    val ite = diffDeviceImsis.
      map{ case (idx, diffdevimsi) =>
      val diffdevimsiTableName = f"diffdevimsi${idx}%02d"
      diffdevimsi.registerTempTable(diffdevimsiTableName)
      val df = sqlContext.sql(
        "select" +
          " dev.month, dev.imsi, dev.net, dev.gender, dev.age," +
          " dev.arpu, dev.dev_vendor, dev.dev_type, dev.bytes, dev.voice," +
          " dev.sms" +
          f", diff.imsi as label" +    // 是否換機, 0: No, 1:Yes
          f" from ${imsiTagetTableName} tg" + // 需要预测的IMSI
          f" left join ${deviceLogTableNamePref}${idx-1}%02d dev  on tg.imsi = dev.imsi" + // 前個月行為
          f" left join ${diffdevimsiTableName} diff on tg.imsi = diff.imsi") // 當月有換機行為IMSI
      df }.
      iterator
    var dataset = ite.next()
    ite.foreach( df => dataset = dataset.unionAll(df))
    dataset
  }

  def multiParamRfCvs( dataTrain:RDD[LabeledPoint], numClasses: Int, catInfo: Map[Int, Int],
                       maxBins: Array[Int], maxDepths: Array[Int], impurities: Array[Int], maxTrees:Array[Int],
                       numFolds: Int = 3)
  : Array[(Array[Int], Array[(RandomForestModel, Array[Double])])] = {
    val seed = 1
    val evaluations = for {
      impurityNum <- impurities
      depth <- maxDepths
      bins <- maxBins
      numTrees <- maxTrees
    } yield {
      val impurity = impurityNum match {
        case 0 => "gini"
        case _ => "entropy"
      }
      val folds: Array[(RDD[LabeledPoint], RDD[LabeledPoint])] = MLUtils.kFold(dataTrain, numFolds, seed)
      val modelMetrics = folds.
        map{ case (training, validation) =>
        training.cache;validation.cache()
        // training model
        println(f"bins=${bins}, depth=${depth}, impurity=${impurity}, trees=${numTrees}, ")
        val model =  RandomForest.trainClassifier(training, numClasses, catInfo, numTrees, "auto", impurity, depth, bins, seed)
        training.unpersist()
        // validatiing model
        val predictLabels = model.predict( validation.map(_.features) ).
          zip( validation.map(_.label) )
        validation.unpersist()
        val metric = new MulticlassMetrics(predictLabels)
        ( model, Array(metric.fMeasure, metric.precision, metric.recall) ) }
      ( Array(bins, depth, impurityNum, numTrees), modelMetrics )
    }
    evaluations
  }


  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    import sqlContext._
    // 需要预测的IMSI
    var path = "hdfs:/user/leoricklin_taoyuan4g/woplus/target/"
    // path = "file:///home/leo/woplus/target/"
    val imsiTargets = tok2Imsi(loadSrc(sc, path)).cache()
    imsiTargets.getStorageLevel.useMemory
    val imsiTagetTableName = "imsitag"
    imsiTargets.registerTempTable(imsiTagetTableName)
    var df = sqlContext.sql(f"select count(1) from ${imsiTagetTableName}")
    var result: Array[Row] = df.collect() // 360698
    // 讀取完整資料
    path = "hdfs:/user/leoricklin_taoyuan4g/woplus/device/"
    val tokens = splitSrc( loadSrc(sc, path), """","""", "IMSI" ).cache
    tokens.getStorageLevel.useMemory
    /* 分隔格式錯誤
    val tokDevice = srcDevice.map{_.split(",")}.
      filter{ case toks => !toks(1).contains("IMSI") }.
      cache
    NAStat.statsWithMissing( tokDevice.map{ toks => Array(toks.size.toDouble)} )
  Array(stats: (count: 6,000,012, mean: 11.008381, stdev: 0.149865, max: 27.000000, min: 11.000000), NaN: 0)
    tokDevice.unpersist(true)
     */
    // 欄位數統計
    NAStat.statsWithMissing( tokens.map{ toks => Array(toks.size.toDouble)} )
    tokens.unpersist(true)
    /*
Array(stats: (count: 6000000, mean: 11.000000, stdev: 0.000000, max: 11.000000, min: 11.000000), NaN: 0)
     */
    val deviceLogAll = tok2DeviceLog(tokens).cache()
    deviceLogAll.getStorageLevel.useMemory
    // 建立屬性對應表
    result = deviceLogAll.select('net).distinct().collect()
    val netMap = result.map{ row => row.getString(0).hashCode }.zipWithIndex.toMap
    /*
Map(1621 -> 0, 1652 -> 1)
     */
    result = deviceLogAll.select('gender).distinct().collect()
    val genderMap = result.map{ row => row.getString(0).hashCode }.zipWithIndex.toMap
    /*
Map(2017367872 -> 0, 366 -> 1, 2097056 -> 2)
     */
    result = deviceLogAll.select('age).distinct().collect()
    val ageMap = result.map{ row => row.getString(0).hashCode }.zipWithIndex.toMap
    /*
Map(47740239 -> 2, 30838 -> 6, -687296262 -> 5, -1566173050 -> 1, 48574422 -> 4, 49497974 -> 0, 47829616 -> 3, 46965670 -> 7, 50421526 -> 8)
     */
    result = deviceLogAll.select('arpu).distinct().collect()
    val arpuMap = result.map{ row => row.getString(0).hashCode }.zipWithIndex.toMap
    /*
Map(0 -> 0, -1306391726 -> 7, 2101070928 -> 2, -1449537636 -> 1, 50421650 -> 3, 1377528339 -> 5, 1957925018 -> 4, 1474882 -> 6)
     */
    // Device屬性
    result = deviceLogAll.select('dev_vendor).distinct().collect()
    val dev_vendorMap = result.map{ row => row.getString(0).hashCode }.zipWithIndex.toMap
    result = deviceLogAll.select('dev_type).distinct().collect()
    val dev_typeMap = result.map{ row => row.getString(0).hashCode }.zipWithIndex.toMap
    //
    result = deviceLogAll.select('bytes).distinct().collect()
    val bytesMap = result.map{ row => row.getString(0).hashCode }.zipWithIndex.toMap
    /*
Map(0 -> 0, -674939055 -> 9, -1355057007 -> 11, 1132485617 -> 5, 45721399 -> 7, 576489553 -> 1, 452367665 -> 4, 1212980289 -> 10, -550817167 -> 3, -1230935119 -> 6, 1256607505 -> 2, 966067099 -> 8)
     */
    val mapping: Map[Int, Map[Int, Int]] = sc.broadcast(
      Map(0 -> Map(0->0), 1->Map(0->0), 2->netMap, 3->genderMap, 4->ageMap,
        5->arpuMap, 6->dev_vendorMap, 7->dev_typeMap, 8->bytesMap )).value
    deviceLogAll.unpersist(true)
    // 讀取各月檔案並轉為DF, : IndexedSeq[ (月份:Int, 行為紀錄:RDD[deviceLog], tableName:String) ]
    val deviceLogTableNamePref = "device"
    val deviceLogEachMonth: IndexedSeq[(Int, RDD[deviceLog], String)] = (1 to 12).
      map{ idx =>
      path = f"hdfs:/user/leoricklin_taoyuan4g/woplus/device/2015${idx}%02d.csv"
      val logs = tok2DeviceLog( splitSrc( loadSrc(sc, path), """","""", "IMSI" ))
      val tablename = f"${deviceLogTableNamePref}${idx}%02d"
      logs.registerTempTable(tablename)
      (idx, logs, tablename) }
    result = sqlContext.sql(f"select * from ${deviceLogTableNamePref}12").limit(5).collect()
    println(result.map{ row => deviceLog2Str(row2DeviceLog(row)) }.mkString("\n"))
    /*
root
 |-- month: integer (nullable = false)
 |-- imsi: string (nullable = true)
 |-- net: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- age: string (nullable = true)
 |-- arpu: string (nullable = true)
 |-- dev_vendor: string (nullable = true)
 |-- dev_type: string (nullable = true)
 |-- bytes: string (nullable = true)
 |-- voice: integer (nullable = false)
 |-- sms: integer (nullable = false)
201512,89b4ad5cd0d3cdeac177ca0bd2170786,3G,ï¿½ï¿½,30-39,50-99,ï¿½ï¿½ï¿½ï¿½,Coolpad T2-W01,0-499,494,93
201512,795dc714d902acf9830f34ed2bcd1054,3G,ï¿½ï¿½,30-39,0-49,ï¿½ï¿½Îª,Che1-CL10,0-499,476,1
201512,8ae17bd692cb4dd11d768d2ae1b10fa7,3G,ï¿½ï¿½,60ï¿½ï¿½ï¿½ï¿½,150-199,Æ»ï¿½ï¿½,A1431,0-499,1601,87
201512,ad4a0e92fb31bbb3c797fb4c3b6bc9a2,3G,ï¿½ï¿½,40-49,300ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½,ï¿½ï¿½Îª,H60-L02,0-499,4,2
201512,0664611530f4443654c87abac1c49169,3G,ï¿½ï¿½,40-49,300ï¿½ï¿½ï¿½ï¿½ï¿½ï¿½,Æ»ï¿½ï¿½,A1528,1000-1499,927,176
     */
    // 從12月開始, 找出當月與上個月手機不同的用戶
    val diffDeviceImsis = findDiffDev(sqlContext, deviceLogEachMonth, imsiTagetTableName, deviceLogTableNamePref)
    diffDeviceImsis(0)._2.printSchema
    /*
root
 |-- imsi: string (nullable = true)
 |-- n_vendor: string (nullable = true)
 |-- n_type: string (nullable = true)
 |-- b_vendor: string (nullable = true)
 |-- b_type: string (nullable = true)
     */
    diffDeviceImsis.map{ case (idx, df) => f"[${idx}%02d]${df.count}" }.mkString("\n")
    /*
[02]22027
[03]21892
[04]18105
[05]19652
[06]20207
[07]18841
[08]18750
[10]17591
[11]34139
[12]15143
     */
    // 驗證
    val diffdevimsiTableName = f"diffdevimsi12"
    df = sqlContext.sql(f"select * from ${diffdevimsiTableName} where imsi = '940d1f5199904143eabc538cde3bdff2'")
    result = df.limit(10).collect()
    println(result.map{ row => Array(
      row.getString(0),row.getString(1),row.getString(2),row.getString(3),row.getString(4)).mkString("][")}.mkString("\n"))
    /*
940d1f5199904143eabc538cde3bdff2][ï¿½ï¿½ï¿½ï¿½][SCH-P709][ï¿½ï¿½Îª][U8800,U8800,IDEOSX5
     */
    // 驗證12月換機用戶的11月紀錄
    diffDeviceImsis.filter{ case (idx, df) => idx == 12}.head._2.registerTempTable(diffdevimsiTableName)
    df = sqlContext.sql("select" +
      " dev.month, dev.imsi, dev.net, dev.gender, dev.age," +
      " dev.arpu, dev.dev_vendor, dev.dev_type, dev.bytes, dev.voice," +
      " dev.sms" +
      f", diff.imsi as label" +    // 是否換機, 0: No, 1:Yes
      f" from ${imsiTagetTableName} tg" + // 需要预测的IMSI
      f" left join ${deviceLogTableNamePref}11 dev  on tg.imsi = dev.imsi" +
      f" left join ${diffdevimsiTableName} diff on tg.imsi = diff.imsi")
    result = df.limit(10).collect()
    result.map{ row => Array(row.getInt(0).toString, row.getString(1), if(row.isNullAt(11)) "0" else "1").mkString(",") }.mkString("\n")
    /*
201511,73a9d4b8708765014e29e7bb507db027,0
201511,b00bb64143128322107cf0174d73eb10,0
201511,940d1f5199904143eabc538cde3bdff2,1
201511,4854db196b6e84672453db9689190bf6,0
201511,19f7bc91ed2830a4e48d33b02b86c4dd,0
201511,620732c02248e641d60b5787aec812b1,0
201511,cf7fc78c2ef8b183f5ac223d2cb9f0e8,0
201511,02d344e58a3b92677a980cd9fbbd6e20,0
201511,37d2769e5113b68a3e52c0d8a4d1d25f,0
201511,844f4ca7e7ed270e1a401bfbdcbc0b31,0
     */
    // 找出換機用戶上的月的行為
    var dataset: SchemaRDD = prev1MDeviceLabel(sqlContext, diffDeviceImsis, imsiTagetTableName, deviceLogTableNamePref)
    dataset.printSchema
    /*
root
 0|-- month: integer (nullable = true)
 1|-- imsi: string (nullable = true)
 2|-- net: string (nullable = true)
 3|-- gender: string (nullable = true)
 4|-- age: string (nullable = true)
 5|-- arpu: string (nullable = true)
 6|-- dev_vendor: string (nullable = true)
 7|-- dev_type: string (nullable = true)
 8|-- bytes: string (nullable = true)
 9|-- voice: integer (nullable = true)
 10|-- sms: integer (nullable = true)
 11|-- label: string (nullable = true)
     */
    val lps = dataset.mapPartitions{ ite =>
      ite.map{ row =>
        new LabeledPoint(
          if (row.isNullAt(11)) 0.0 else 1.0
          , Vectors.dense(
            mapping(2)(row.getString(2).hashCode).toDouble, // 0,net
            mapping(3)(row.getString(3).hashCode).toDouble, // 1,gender
            mapping(4)(row.getString(4).hashCode).toDouble, // 2,age
            mapping(5)(row.getString(5).hashCode).toDouble, // 3,arpu
            mapping(8)(row.getString(8).hashCode).toDouble, // 4,bytes
            row.getInt(9).toDouble,
            row.getInt(10).toDouble ) )} }.cache
    lps.getStorageLevel.useMemory
    NAStat.statsWithMissing( lps.map{lp => Array(lp.features.size)} )
    /*
Array(stats: (count: 3606980, mean: 7.000000, stdev: 0.000000, max: 7.000000, min: 7.000000), NaN: 0)
     */
    //
    val numClasses = 2
    val catInfo = Map(
      0->2, // netMap.size = 2
      1->3, // genderMap.size = 3
      2->9, // ageMap.size = 9
      3->8, // arpuMap.size = 8
      4->12) // bytesMap.size = 12
    val maxBins = Array(20)
    val maxDepth = Array(20, 30)
    val impurities = Array(1)
    val maxTrees = Array(32)
    val numFolds = 3
    val seed = 1
    // multiParamRfCvs() takes over 2 days
    /*
    val cvModels: Array[(Array[Int], Array[(RandomForestModel, Array[Double])])] = multiParamRfCvs(lps, numClasses, catInfo, maxBins, maxDepth, impurities, maxTrees, numFolds)
     */
    // 建立 train / cv dataset
    val Array(dataTrain, dataCV) = lps.randomSplit(Array(0.8, 0.1))
    dataTrain.cache(); dataTrain.getStorageLevel.useMemory;
    dataCV.cache();dataCV.getStorageLevel.useMemory;
    dataTrain.map{ lp => lp.label }.countByValue()
    dataCV.map{ lp => lp.label }.countByValue() // Map(0.0 -> 378289, 1.0 -> 23114)
    lps.unpersist(true)
    // fits & evaluates model
    val model: RandomForestModel = RandomForest.trainClassifier(dataTrain, numClasses, catInfo, 24, "auto", "entropy", 30, 30, seed)
    /*
dataTrain.count = 3,205,577
4 cores, 16GB MEM per executor, 8 executors, Driver MEM = 16GB => takes 10 hours to finish one RF model training
     */
    val predictCV = dataCV.map{ cv => (model.predict(cv.features), cv.label)}
    val metrics = new MulticlassMetrics(predictCV)
    /*
    metrics.confusionMatrix // takes 5 mins
378284.0  5.0
23109.0   5.0
    metrics.precision // takes 6 mins
= 0.9424169724690648
     */
    // 找出 target 用戶的12月行為
    val dataTest = sqlContext.
      sql(
        f"select dev.month,dev.imsi,dev.net,dev.gender,dev.age," +
          f"dev.arpu,dev.dev_vendor,dev.dev_type,dev.bytes,dev.voice,dev.sms" +
          f" from ${imsiTagetTableName} tag inner join" +
          f" ${deviceLogTableNamePref}12 dev on tag.imsi = dev.imsi").
      map{ row => deviceLog2Vector(row2DeviceLog(row), mapping) }.cache
    dataTest.getStorageLevel.useMemory
    dataTest.count // = 360698
    val predictTest = dataTest.map{ case (imsi, features) => (imsi, features, model.predict(features)) }
    path = "hdfs:/user/leoricklin_taoyuan4g/woplus/target.predict"
    predictTest.map{ case (imsi, features, pred) => f"${imsi},${pred}" }.saveAsTextFile(path)
    /*
root
 |-- month: integer (nullable = false)
 |-- imsi: string (nullable = true)
 |-- net: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- age: string (nullable = true)
 |-- arpu: string (nullable = true)
 |-- dev_vendor: string (nullable = true)
 |-- dev_type: string (nullable = true)
 |-- bytes: string (nullable = true)
 |-- voice: integer (nullable = false)
 |-- sms: integer (nullable = false)
     */
    //
    imsiTargets.unpersist(true)
  }
}
