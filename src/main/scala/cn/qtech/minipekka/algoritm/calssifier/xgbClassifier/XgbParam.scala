package cn.qtech.minipekka.algoritm.calssifier.xgbClassifier

import cn.qtech.minipekka.algoritm.algBase.Parameters
import cn.qtech.minipekka.common.path.HdfsPath

class XgbParam extends Parameters {

  override val algName: String = "XgbClassifier"


  override var modelSavePath: HdfsPath = _
  override var resSavePath: HdfsPath = _
  override var trainDataPath: HdfsPath = _
  override var testDataPath: HdfsPath = _
  override var loadModelPath: HdfsPath = _
  override var splitRate: Double = _
  override var productLine: String = _

  override var modelParams: Map[String, Any] = _


  /**
   * 通过解析json字符串的方式填充参数
   *
   * @param jsonString 参数key和内容组成的json字符创
   * @return
   */
  override def updateFromJsonString(jsonString: String): Boolean = ???

  /**
   * 通过读取hdfs中的存储的内容填充参数
   *
   * @param hdfsPath
   * @return
   */
  override def loadFromHdfs(hdfsPath: String): Boolean = ???

  /**
   * 把设定好的参数存储到hdfs
   *
   * @param hdfsPath
   * @return
   */
  override def saveToHdfs(hdfsPath: String): Boolean = ???

  /**
   * 参数内容验证
   *
   * @return 如果成功返回"ok"或者空，失败返回错误信息
   */
  override def validation(): String = ???

  /** 当前 job的状态
   * “deploy": 已经部署(给定 testDataPath、loadModelPath参数, resSavePath可选)
   * "train": 训练调参阶段(给定 trainDataPath、splitRate参数, modelSavePath可选)
   * "trainAndPredict" 训预一体(给定trainDataPath、testDataPath, 其他参数可选)
   */
}
