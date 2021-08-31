package cn.qtech.minipekka.project.anglepredict

import cn.qtech.minipekka.common.datareader.SparkSqlReader
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * author   :  jdi146
 * contact  :  jdi147.com@gmail.com
 * date     :  2021/3/31 11:29
 */


/** 计算pad和lead各自点的相邻两点之间的二维空间欧式距离, 输入数据经处理后与标准模板根据阈值进行对比,给出预测结果
 *
 * ---------------------------------------------------------------------------------------------------------------------
 * demo:
 * val stdModelPath = "/FileStore/tables/stdModel.csv"
 * val trainDataPath = "/FileStore/tables/trainData.csv"
 * val algorithm = new AngleDetect(decimalPlaces = 2)
 * val processedTrnRDD = AngleETL.preprocessing(rawTrainData = trainDF)
 * algorithm.fit(stdModelPath)
 * val predictRes: DataFrame = algorithm.predict(processedTrnRDD)
 * predictRes.show(20)
 * predictRes.show(20, false)
 * ---------------------------------------------------------------------------------------------------------------------
 * input:
 * ***** 1. fit: stdModelPath(json, scv, parquet): DF[(lineNmb(Int), (leadDiff(Double), padDiff(Double), leadThreshold(Double), padThreshold(Double))]
 * ***** 2. predict:  RDD[sid, (lineNmb(Int), leadX(Double), leadY(Double), padX(Double), padY(Double))]
 *
 * output: predict func: DF[equipmentID(String), mcID(String), datetime(String), label(0/1/2), description(String)]
 * description:
 *        1. qualified,此时label为0, 表示合格
 *           2. 当label为1时, description 是对异常线的相关描述
 *           3. 当label为2时, description 是对缺线数量的描述
 *           ---------------------------------------------------------------------------------------------------------------------
 *           note:
 *           ***** 1. 表中模板中的threshold>=0
 *           ***** 2. decimalPlaces为异常线的描述值中的保留小数位
 *           ***** 3. 必须在进行fit后作predict
 *           ---------------------------------------------------------------------------------------------------------------------
 */


class AngleAlg(decimalPlaces: Int = 2, delimiter: String = "_") extends Serializable {

  /**
   * modelLinesNum, modelLinesNmb, model 在 调用fit()方法后进行初始化
   */
  var modelLinesNum: Int = _
  var modelLinesNmb: Seq[Int] = _
  var model: Seq[(Int, (Double, Double, Double, Double))] = _
  val ss: SparkSession = SparkSession.builder().config(new SparkConf()).getOrCreate()


  //
  //  def checkTrnData(data: RDD[(String, Seq[(Int, Double, Double, Double, Double)])): Unit = {
  //
  //  }


  /** 读取标准模板
   * 根据输入stdModel路径读取stdModel并作 collect to  Seq
   *
   * @param stdModelPath : 输入stdModel 路径, csv/parquet/json
   * @return : Seq[(lineNmb(Int), (leadDiff(Double), padDiff(Double), leadThreshold(Double), padThreshold(Double))]
   */
  private def getStdModel(stdModelPath: String): Seq[(Int, (Double, Double, Double, Double))] = {

    import ss.implicits._
    val pattern = ".*\\.".r
    val fileType = pattern.replaceAllIn(stdModelPath, "")

    val data: DataFrame = fileType match {
      case "csv" => SparkSqlReader.readByCsv(path = stdModelPath, ss = ss)
      case "parquet" => SparkSqlReader.readByParquet(path = stdModelPath, ss = ss)
      case "json" => SparkSqlReader.readByJson(path = stdModelPath, ss = ss)
      case _ => throw new IllegalArgumentException("请输入符合要求的文件类型: 【csv/parquet/json】")
    }

    val df = data.select($"lineNmb".cast("int"), $"leadDiff", $"padDiff", $"leadThreshold".cast("double"), $"padThreshold".cast("double"))
    val res = df.rdd
      .map(r => (r.getAs[Int]("lineNmb"), (r.getAs[Double]("leadDiff"),
        r.getAs[Double]("padDiff"), r.getAs[Double]("leadThreshold"), r.getAs[Double]("padThreshold"))))
      .collect()
      .sortBy(_._1)
    res
  }


  /** 执行getStdModel方法, 并初始化 model, modelLinesNmb, modelLinesNum
   *
   * @param stdModelPath : 标准模板路径
   */
  def fit(stdModelPath: String): Unit = {
    model = getStdModel(stdModelPath = stdModelPath)
    modelLinesNmb = model.map(_._1)
    modelLinesNum = model.length
  }


  private def splittingSid(sid: String): Array[String] = {
    sid.split(delimiter)
  }


  /** 预测
   *
   * 将原始数据分割成缺线和非缺线两边部分分别进行predict后, 返回concat结果
   *
   * sid: eqID + delimiter + mcID + delimiter + outPutTime + delimiter + String.valueOf(id)
   *
   * @param trainData : RDD[(sid(String), (lineNmb(Int), leadX(Double), leadY(Double), padX(Double), padY(Double)))]
   * @return 预测结果 DF[equipmentID(String), mcID(String), datetime(String), label(0/1/2), description(String)]
   */
  def predict(trainData: RDD[(String, (Int, Double, Double, Double, Double))]): DataFrame = {

    import ss.implicits._

    val data = trainData
      .groupByKey()
      .map(r => (r._1, r._2.toSeq.distinct.sortBy(_._1)))

    // jdi147.com: 少线数据
    val lackLineData = data.filter(_._2.size < modelLinesNum)
    // jdi147.com: 不少线数据
    val fullLineData = data.filter(_._2.size == modelLinesNum)

    val lackLinePre = predictLackLine(data = lackLineData)
    val fullLinePre = predictFullLine(data = fullLineData)

    val res = lackLinePre.union(fullLinePre)
    res.toDF("equipmentid", "mcid", "datetime", "code", "description")
  }


  /** Threshold的比较准则
   *
   * @param modelLeadDiff      : 标准模板的 leadDiff
   * @param modelPadDiff       : 标准模板的 padDiff
   * @param lineLeadDiff       : 样本的 leadDiff
   * @param linePadDiff        : 样本的 padDiff
   * @param modelLeadThreshold : 标准模板的 leadThreshold
   * @param modelPadThreshold  : 标准模板的 padThreshold
   * @return : 0/1, 0表示合格, 1表示不合格
   */
  private def getState(modelLeadDiff: Double, modelPadDiff: Double, lineLeadDiff: Double, linePadDiff: Double, modelLeadThreshold: Double, modelPadThreshold: Double): (Int, Double, Double) = {

    val leadOffset = math.abs(modelLeadDiff - lineLeadDiff)
    val padOffset = math.abs(modelPadDiff - linePadDiff)
    if (leadOffset > modelLeadThreshold || padOffset > modelPadThreshold)
      (1, leadOffset, padOffset)
    else
      (0, leadOffset, padOffset)
  }


  /** 通过给定的标准模板中leadThreshold和padThreshold来判断全线的单个样本
   *
   * @param diffData : Seq[(lineNmb(Int), leadDis, padDis)]
   * @return (label(Int), unqualifiedLines(Seq[unqualifiedLineNmb(Int), leadOffset, padOffset)]))
   */
  private def singlePreByThreshold(diffData: Seq[(Int, (Double, Double))]): (Int, Seq[(Int, Double, Double)]) = {

    // jdi147.com: linesState: Seq[(lineNmb, (state, leadOffset, padOffset))]
    val linesState = diffData.indices.map(idx => {
      val lineNmb = diffData(idx)._1
      val state = getState(modelLeadDiff = model(idx)._2._1, modelPadDiff = model(idx)._2._2, lineLeadDiff = diffData(idx)._2._1, linePadDiff = diffData(idx)._2._2,
        modelLeadThreshold = model(idx)._2._3, modelPadThreshold = model(idx)._2._4)

      (lineNmb, state)
    }).toSeq

    val unqualifiedLines = linesState.filter(_._2._1 == 1)
    val label = if (unqualifiedLines.isEmpty) 0 else 1

    (label, unqualifiedLines.map(r => (r._1, r._2._2, r._2._3)))
  }


  /** 预测少线数据
   *
   * @param data : 少线数据, RDD[sid(String), lineNmb(Int), leadX(Double), leadY(Double), padX(Double), padY(Double)]
   * @return 预测结果, RDD[(equipmentID(String), mcID(String), datetime(String), label(2),description(Int))]
   */
  private def predictLackLine(data: RDD[(String, Seq[(Int, Double, Double, Double, Double)])]): RDD[(String, String, String, Int, String)] = {

    val res = data.mapPartitions(
      partition => {
        partition.map(
          r => {
            val splitSid = splittingSid(r._1)
            val linesNum = r._2.length
            (splitSid(0), splitSid(1), splitSid(2), 2, s"少线: 【实际/标准: $linesNum/$modelLinesNum】")
          })
      })
    res
  }


  /** 对一个非缺线的不合格产品预测结果做description
   *
   * @param lackedLines : 一个非缺线的不合格产品, Seq[(lineNmb(Int), leadOffset(Double), padOffset(Double))]
   * @return str
   */
  private def fullCode2doc(lackedLines: Seq[(Int, Double, Double)]): String = {
    val docSep = lackedLines.map(line =>
      s"lineNmb${line._1}" +
        "_leadOffset_" + s"%1.${decimalPlaces}f".format(line._2) +
        "_padOffset_" + s"%1.${decimalPlaces}f".format(line._3)
    )
    docSep.mkString("|")
  }


  /** 非少线数据预测
   *
   * @param data : 非少线数据, RDD[sid(String), lineNmb(Int), leadX(Double), leadY(Double), padX(Double), padY(Double)]
   * @return : 预测结果,  RDD[(equipmentID(String), mcID(String), datetime(String), label(0/1), description(String))]
   */
  private def predictFullLine(data: RDD[(String, Seq[(Int, Double, Double, Double, Double)])]): RDD[(String, String, String, Int, String)] = {

    val res = data.mapPartitions(partition => {
      partition.map(r => {
        val sid = r._1
        val linesCoordinate = r._2
        val diff = __doDiff(linesCoordinate)

        // jdi147.com: predicts : (label(Int), unqualifiedLines(Seq[(unqualifiedLineNmb, leadOffset, padOffset)]))
        val predicts = singlePreByThreshold(diff)
        val splitSid = splittingSid(sid)

        val describeDoc: String = predicts._1 match {
          case 1 => fullCode2doc(predicts._2)
          case 0 => "qualified"
        }
        (splitSid(0), splitSid(1), splitSid(2), predicts._1, describeDoc)
      })
    })

    res
  }


  /** 计算二维空间两点欧式距离
   *
   */
  def __euclideanDistance(x1: Double, y1: Double, x2: Double, y2: Double): Double = {

    val xDiff = x1 - x2
    val yDiff = y1 - y2
    math.sqrt(xDiff * xDiff + yDiff * yDiff)
  }


  /** pad和lead各自点的相邻两点之间作shift并计算欧式距离
   *
   * @param linesCoordinate : 被作业的sid的所有线的pad和lead的二维坐标, (LineNmb(线编号), leadX, leadY, padX, padY)
   * @return Seq[(lineNmb(Int), leadDis, padDis)]
   */
  def __doDiff(linesCoordinate: Seq[(Int, Double, Double, Double, Double)]): Seq[(Int, (Double, Double))] = {

    // jdi147.com: 获取shift
    val shift = linesCoordinate.drop(1)

    // jdi147.com: 将原始数据中的坐标值与shift中的坐标中作差, 并计算欧式距离
    val diff = shift.indices.map(idx => {
      val lineNmb = shift(idx)._1
      val leadDiff = __euclideanDistance(x1 = shift(idx)._2, y1 = shift(idx)._3, x2 = linesCoordinate(idx)._2, y2 = linesCoordinate(idx)._3)
      val padDiff = __euclideanDistance(x1 = shift(idx)._4, y1 = shift(idx)._5, x2 = linesCoordinate(idx)._4, y2 = linesCoordinate(idx)._5)

      (lineNmb, (leadDiff, padDiff))
    })

    // jdi147.com: 添加首行元
    val res = (1, (0.0, 0.0)) +: diff
    res

  }

}
