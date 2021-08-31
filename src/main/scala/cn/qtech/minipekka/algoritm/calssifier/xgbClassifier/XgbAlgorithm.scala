package cn.qtech.minipekka.algoritm.calssifier.xgbClassifier

import cn.qtech.minipekka.algoritm.algBase.Algorithm
import ml.dmlc.xgboost4j.scala.spark.{XGBoostClassifier, XGBoostClassificationModel}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

class XgbAlgorithm(dataMap: Map[String, DataFrame]) extends Algorithm {

  override protected val parameters: XgbParam = new XgbParam
  override protected val algUtil: XgbAlgUtil = new XgbAlgUtil

  override protected var trainDF: DataFrame = _
  override protected var testDF: DataFrame = _
  override protected var resDF: DataFrame = _


  val ss: SparkSession = SparkSession.builder().config(new SparkConf()).getOrCreate()


  override protected def featuring(df: DataFrame): DataFrame = {
    df
  }


  override protected def preprocessing(trainDF: DataFrame, testDF: DataFrame): (DataFrame, DataFrame) = {

    val allDF = trainDF.join(testDF)
    val handledDF = featuring(allDF)

    // split
    (handledDF, handledDF)

  }

  override protected def preprocessing(df: DataFrame): DataFrame = {
    featuring(df)
  }


  override protected def beforeInvoke(): Unit = {

    trainDF = dataMap("train")

    parameters.productLine match {
      case "train" => logger.info("productLine: train, dataSet: [trainDF]")
      case "loadAndPredict" => logger.info("productLine: loadAndPredict, dataSet: [testDF]")
      case "trainAndPredict" => {
        testDF = dataMap("test")
        logger.info("productLine: trainAndPredict, dataSet: [trainDF, testDF]")
      }
      case _ => throw new RuntimeException("The productLine should in (train, loadAndPredict, trainAndPredict)")
    }

  }


  override protected def invoke(): Unit = {


    var xgbClassificationModel: XGBoostClassificationModel = null

    parameters.productLine match {
      case "train" => {
        val xgbClassifier = new XGBoostClassifier(parameters.modelParams)
          .setFeaturesCol("features")
          .setLabelCol("label")
        xgbClassificationModel = xgbClassifier.fit(trainDF)
        resDF = xgbClassificationModel.transform(trainDF)

      }

      case "loadAndPredict" => {

        xgbClassificationModel = algUtil.loadModel(parameters.loadModelPath)
        resDF = xgbClassificationModel.transform(trainDF)

      }

      case "trainAndPredict" => {
        val xgbClassifier = new XGBoostClassifier(parameters.modelParams)
          .setFeaturesCol("features")
          .setLabelCol("label")
        xgbClassificationModel = xgbClassifier.fit(trainDF)
        resDF = xgbClassificationModel.transform(trainDF)
      }
    }

  }

  override protected def afterInvoke(): Unit = {
//    resDF = _
//    //    model.saveRes(resDF)
//    algUtil.saveModel(parameters.modelSavePath)

  }



}
