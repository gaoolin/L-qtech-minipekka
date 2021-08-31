package cn.qtech.minipekka.algoritm.algBase

import cn.qtech.minipekka.common.path.Path
import org.apache.spark.sql.DataFrame
import org.slf4j.LoggerFactory


/**
 *  1. 读取训练数据(包含验证数据)、测试数据
 *     2. 保存预测结果
 *     3. 保存模型
 *     4. 解析模型参数
 */
trait AlgUtil extends Serializable {


  private val logger = LoggerFactory.getLogger(this.getClass)

  protected def getTrainData(path: Path): DataFrame

  protected def getTestData(path: Path): DataFrame

  protected def saveRes(path: Path): Boolean



}
