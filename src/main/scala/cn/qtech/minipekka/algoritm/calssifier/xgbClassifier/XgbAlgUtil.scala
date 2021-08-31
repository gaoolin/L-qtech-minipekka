package cn.qtech.minipekka.algoritm.calssifier.xgbClassifier

import cn.qtech.minipekka.algoritm.algBase.AlgUtil
import cn.qtech.minipekka.common.path.{HdfsPath, Path}
import org.apache.spark.sql.DataFrame
import ml.dmlc.xgboost4j.scala.spark.XGBoostClassificationModel

class XgbAlgUtil extends AlgUtil {


  override protected def getTrainData(path: Path): DataFrame = ???

  override protected[xgbClassifier] def getTestData(path: Path): DataFrame = ???

  override protected[xgbClassifier] def saveRes(path: Path): Boolean = ???


  protected[xgbClassifier] def saveModel(model: XGBoostClassificationModel, path: HdfsPath): Unit = {
    try {
      model.write.overwrite().save(path.getHdfsPath)
    } catch {
      case e: Exception => throw new NoSuchFieldException("save model failed!")
    }
  }

  protected[xgbClassifier] def loadModel(path: HdfsPath): XGBoostClassificationModel = {
    XGBoostClassificationModel.load(path.getHdfsPath)
  }

}
