package cn.qtech.minipekka.common.datareader

import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Properties

/**
 * author   :  jdi146
 * contact  :  jdi147.com@gmail.com
 * date     :  2021/4/2 20:40
 */


object SparkSqlReader extends Serializable {


  def readByCsv(path: String, ss: SparkSession): DataFrame = {

    val df: DataFrame = ss.read.format("csv")
      .option("header", "true")
      .option("mode", "FAILFAST")
      .option("inferSchema", "true")
      .load(path)
    df
  }

  def readByParquet(path: String, ss: SparkSession): DataFrame = {
    val df = ss.read.format("parquet").load(path)
    df
  }

  def readByJson(path: String, ss: SparkSession): DataFrame = {
    val df = ss.read.json(path)
    df
  }


  def readByJBDC(connectionProperties: Properties, jdbcURL: String, tableName: String, ss: SparkSession): DataFrame = {

    val jdbcDF = ss.read
      .jdbc(jdbcURL, tableName, connectionProperties)
    jdbcDF
  }

  def readByJBDC1(params: Map[String, String], ss: SparkSession): DataFrame = {
    val df = ss.read
      .format("jdbc")
      .option("url", "jdbc:postgresql:dbserver")
      .option("dbtable", "schema.tablename")
      .option("user", "username")
      .option("password", "password")
      .load()
    df
  }
}
