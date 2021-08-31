package cn.qtech.minipekka.algoritm.algBase

import cn.qtech.minipekka.common.path.HdfsPath

trait Parameters extends Serializable {

  val algName: String


  var modelSavePath: HdfsPath
  var resSavePath: HdfsPath
  var trainDataPath: HdfsPath
  var testDataPath: HdfsPath
  var loadModelPath: HdfsPath
  var splitRate: Double

  // json 格式的model参数
  var modelParams: Map[String,Any]

  /** 当前 job的状态
   *  “deploy": 已经部署(给定 testDataPath、loadModelPath参数, resSavePath可选)
   *  "train": 训练调参阶段(给定 trainDataPath、splitRate参数, modelSavePath可选)
   *  "trainAndPredict" 训预一体(给定trainDataPath、testDataPath, 其他参数可选)
   */
  var productLine: String

  /**
   * 通过解析json字符串的方式填充参数
   *
   * @param jsonString 参数key和内容组成的json字符创
   * @return
   */
  def updateFromJsonString(jsonString: String): Boolean

  /**
   * 通过读取hdfs中的存储的内容填充参数
   *
   * @param hdfsPath
   * @return
   */
  def loadFromHdfs(hdfsPath: String): Boolean

  /**
   * 把设定好的参数存储到hdfs
   *
   * @param hdfsPath
   * @return
   */
  def saveToHdfs(hdfsPath: String): Boolean

  /**
   * 参数内容验证
   *
   * @return 如果成功返回"ok"或者空，失败返回错误信息
   */
  def validation(): String


  def getModelParams: String={
    modelParams.toString
  }

}
