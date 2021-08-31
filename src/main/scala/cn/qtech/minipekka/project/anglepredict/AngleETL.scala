package cn.qtech.minipekka.project.anglepredict

import cn.qtech.minipekka.utils.SendMail
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import play.api.libs.mailer.{Attachment, Email, SMTPMailer}


object AngleETL extends Serializable {

  val delimiter: String = "_"


  /** 对数据进行预处理
   *
   * @param rawTrainData : 原始数据 DF[linenmb(Int), equipmentid(String), mcid(String), datetime(String), leadx(String), leadY(String), padx(String), padY(String)]
   * @param ss           : SparkSession
   * @return RDD[(key(自设), (lineNmb, leadX, leadY, padX, padY), mcID)]
   */
  def preprocessing(rawTrainData: DataFrame, ss: SparkSession)
  : RDD[(String, (Int, Double, Double, Double, Double), String)] = {
    var id: Long = 0
    var outPutTime = ""

    import ss.implicits._
    /** convert dataType */
    val castDF = rawTrainData.select($"linenmb", $"equipmentid", $"mcid", $"datetime",
      $"leadx".cast("double"),
      $"leady".cast("double"),
      $"padx".cast("double"),
      $"pady".cast("double")
    )

    /** 先做初步的主键, 目的: 方便后面的主键map操作， 方便拉取到主节点选取更少的列,减少主节点内存压力, 方便下一步数据排序
     * rawTrainRDD: (key, (lineNmb, leadX, leadY, padX, padY), (eqID, mcID, datetime), mcID)
     * key = eqID + delimiter + mcID + delimiter + datetime
     */
    val rawTrainRDD = castDF.rdd.map(r => {
      (r.getAs[Int]("linenmb"),
        r.getAs[String]("equipmentid"), r.getAs[String]("mcid"), r.getAs[String]("datetime"),
        r.getAs[Double]("leadx"), r.getAs[Double]("leady"), r.getAs[Double]("padx"), r.getAs[Double]("pady")
      )
    })
      .map(record => {
        val lineNmb = record._1
        val eqID = record._2
        val mcID = record._3
        val datetime = record._4
        val leadX = record._5
        val leadY = record._6
        val padX = record._7
        val padY = record._8
        val key = eqID + delimiter + mcID + delimiter + datetime
        (key, (lineNmb, leadX, leadY, padX, padY), (eqID, mcID, datetime), mcID)
      })


    /** 对数据进行排序以自设主键, 得到 主键 MAP
     * sort: (eqID + delimiter + mcID + delimiter + datetime, lineNmb)
     * 每组数据的时间点取线编号为1的时间点
     */
    val keyMap = rawTrainRDD.map(r => (r._1, r._2._1, r._3._3))
      .collect()
      .distinct
      .sortBy(r => (r._1, r._2))
      .map(r => {
        val key = r._1
        val datetime = r._3
        val lineNmb = r._2
        if (lineNmb == 1) {
          id += 1
          outPutTime = datetime
        }
        (key, outPutTime + delimiter + String.valueOf(id))
      })
      .toMap

    val res = rawTrainRDD
      .map(record => {
        val id = keyMap.getOrElse(record._1, "nan")
        val primaryKey = record._3._1 + delimiter + record._3._2 + delimiter + id
        (primaryKey, record._2, record._4.split("#")(0).replace(" ", ""))
      })

    res
  }


  /** 将预测结果union
   *
   * @param dataFrames : Seq[DF]
   * @return
   */
  def unionDataFrame(dataFrames: Seq[DataFrame]): DataFrame = {
    var baseDF = dataFrames.head
    for (idx <- 1 until dataFrames.length) {
      baseDF = baseDF.union(dataFrames(idx))
    }
    baseDF
  }


  /** 运行算法
   *
   * @param trainData     :  RDD[(primaryKey, lineNmb, leadX, leadY, padX, padY))]
   * @param stdModelPath  : 标准模板HDFS路径
   * @param decimalPlaces : 显示结果保留的小数位, 默认2
   * @return
   */
  def algRun(trainData: RDD[(String, (Int, Double, Double, Double, Double))], stdModelPath: String, ss: SparkSession, decimalPlaces: Int = 2): DataFrame = {
    val hdfs: FileSystem = FileSystem.get(new Configuration)
    if (!hdfs.exists(new Path(stdModelPath))) {

      println(
        s"""
           | --------------------------------------------------- 【warning】 -------------------------------------------------
           |                                                                                                                 |
           |                                                                                                                 |
           |                                                【缺少标准模板: $stdModelPath】                                     |
           |                                                                                                                 |
           |                                                                                                                 |
           |                                                                                                                 |
           |----------------------------------------------------- 【warning】 -------------------------------------------------

        """.stripMargin)

      val schema = (new StructType)
        .add(StructField("equipmentid", StringType))
        .add(StructField("mcid", StringType))
        .add(StructField("datetime", StringType))
        .add(StructField("code", IntegerType))
        .add(StructField("description", StringType))
      val emptyDf = ss.createDataFrame(ss.sparkContext.emptyRDD[Row], schema)
      emptyDf
    }
    else {
      val algorithm = new AngleAlg(decimalPlaces = decimalPlaces, delimiter = AngleETL.delimiter)
      algorithm.fit(stdModelPath)
      val predictedRes: DataFrame = algorithm.predict(trainData)
      predictedRes
    }
  }


  /** 读取训练数据
   *
   * @param kuduMaster : KuduMaster 地址
   * @param tableName  : 表名
   * @param ss         : SparkSession
   * @param filterStr  : 过滤条件
   * @return : DF[linenmb(Int), equipmentid(String), mcid(String), datetime(String), leadx(String), leady(String), padx(String), pady(String)]
   */
  def getTrainData(kuduMaster: String, tableName: String, ss: SparkSession, filterStr: String): DataFrame = {
    val trainDF = ss.read.options(Map("kudu.master" -> kuduMaster, "kudu.table" -> tableName)).format("kudu").load
    trainDF.createOrReplaceTempView("kuduTable")
    val filterCmd = s"select * from kuduTable where linenmb > 0 and $filterStr"
    val selectDF = ss.sql(filterCmd)
    selectDF
  }


  /** 读取上次job调度时拉取数据中最大时间戳, 以作本次job调度过滤数据使用
   *
   * @param path : 读取路径
   * @return
   */
  def readPreRunDate(path: String, ss: SparkSession): String = {
    val lines = ss.read.textFile(path).rdd.collect()
    val datetime = lines.head
    datetime
  }


  /** 写入本次job调度时拉取数据中最大时间戳, 以作下次job调度过滤拉取数据使用
   *
   * @param datetime : 本次job调度时拉取数据中最大时间戳
   * @param path     : 写入路径
   */
  def writeDate2txt(datetime: DataFrame, path: String, ss: SparkSession): Unit = {
    datetime.write.mode("Overwrite").text(path)
  }


  /** 结果回流数据胡
   *
   * @param df         : DF[equipmentID(String), mcID(String), datetime(String), label(0/1/2), description(String)]
   * @param kuduMaster : kudu master
   * @param tableName  : table name
   * @param ss         : SparkSession
   */
  def writeRes2Kudu(df: DataFrame, kuduMaster: String, tableName: String, ss: SparkSession): Unit = {

    df.createOrReplaceTempView("tmp1")
    ss.read.format("kudu")
      .options(Map("kudu.master" -> kuduMaster,
        "kudu.table" -> tableName))
      .load
      .createOrReplaceTempView("ads_angle_rt_data_bak")

    ss.sql(
      s"""
         |insert into $tableName select * from tmp1
         |""".stripMargin)
  }


  /** 预测结果处理成邮件报警格式
   *
   * 邮件发送的数据中只含NG的
   *
   * @param predictDF   : DF[equipmentID(String), mcID(String), datetime(String), label(0/1/2), description(String)]
   * @param ss          : SparkSession
   * @param equipmentDF : DF[机台号(String), 盒子编码(String), 厂(String), 区(String)]
   * @return DF("盒子编码", "机型", "对比结果", "时间", "一级详细信息", "二级详细信息")
   */
  def predRes2Mail(predictDF: DataFrame, ss: SparkSession, equipmentDF: DataFrame): DataFrame = {
    import ss.implicits._

    //    .split("#")(0)
    val showRes = predictDF.rdd
      .map(r => (r.getAs[String]("equipmentid"), r.getAs[String]("mcid"),
        r.getAs[String]("datetime"), r.getAs[Int]("code"), r.getAs[String]("description")))
      .map(r => {
        val codeNmb = r._4
        val boxID = r._1
        val mcID = r._2
        val detailsDescription = r._5
        val datetime = r._3

        val codeStr = if (codeNmb == 1 || codeNmb == 2) {
          "NG"
        }
        else {
          "OK"
        }

        val description = if (codeNmb == 1) {
          "不合格"
        }
        else if (codeNmb == 2) {
          "缺线"
        }
        else {
          "正常"
        }

        (boxID, mcID, codeStr, datetime, description, detailsDescription)
      })
    val mailDF = showRes.toDF("盒子编码", "机型", "对比结果", "时间", "一级详细信息", "二级详细信息")
    val res = mailDF.join(equipmentDF, equipmentDF("盒子编码") === mailDF("盒子编码")).drop(equipmentDF("盒子编码"))
    res.filter($"对比结果" =!= "OK")
  }


  /** 结果回流email
   *
   * @param host           : 邮件host
   * @param port           : 端口号
   * @param from           : 发件人邮箱
   * @param fromPwd        : 发件人邮箱密码
   * @param to             : 收件人邮箱, Seq
   * @param data           : 数据, 固定的RDD格式
   * @param subject        : 邮件主题
   * @param attachmentName : 附件名称
   */
  def sendMail(host: String, port: Int, from: String, fromPwd: String, to: Seq[String],
               data: RDD[(String, String, String, String, String, String)],
               subject: String, attachmentName: String = "附件"): Unit = {
    val attachment: Attachment = SendMail.createAttachment(attachmentName, rawData = data)
    val emails: Email = SendMail.createEmail(subject, from, to, attachments = Seq(attachment))
    val myMailer: SMTPMailer = SendMail.createMailer(host, port, from, fromPwd)
    myMailer.send(emails)
  }

}

