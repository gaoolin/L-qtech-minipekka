package cn.qtech.minipekka.algoritm.algBase

import org.apache.spark.sql.DataFrame
import org.slf4j.{Logger, LoggerFactory}


trait Algorithm extends Serializable {

  protected val parameters: Parameters
  protected val algUtil: AlgUtil

  // 定义输入数据、输出数据
  protected var trainDF: DataFrame
  protected var testDF: DataFrame
  protected var resDF: DataFrame


  protected val logger: Logger = LoggerFactory.getLogger(this.getClass)

  final def run(): Unit = {
    val className = this.getClass.getSimpleName
    //    logger.info(s"=====Parameter Validation Start [$className]====")
    //    paramValidation()
    logger.info(s"=====Before Invoke Start [$className]====")
    beforeInvoke()
    logger.info(s"=====Invoke Start [$className]====")
    invoke()
    logger.info(s"=====After Invoke Start [$className]====")
    afterInvoke()
    logger.info(s"=====Run Method Finish [$className]====")
  }

  protected def featuring(df: DataFrame): DataFrame

  protected def preprocessing(trainDF: DataFrame, testDF: DataFrame): (DataFrame, DataFrame)

  protected def preprocessing(df: DataFrame): DataFrame


  protected def beforeInvoke(): Unit

  protected def invoke(): Unit

  protected def afterInvoke(): Unit


}
