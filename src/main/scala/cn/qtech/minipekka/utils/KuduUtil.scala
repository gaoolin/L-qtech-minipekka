package cn.qtech.minipekka.utils

import org.apache.kudu.client._
import org.apache.kudu.spark.kudu._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.{Logger, LoggerFactory}


/** kudu spark数据交互
 *
 * @param kuduMaster : kudu master 节点
 */
class KuduUtil(kuduMaster: String, ss: SparkSession) extends Serializable {

  val sc: SparkContext = ss.sparkContext
  val kuduContext = new KuduContext(kuduMaster, sc)

  //  import ss.implicits._

  val logger: Logger = LoggerFactory.getLogger(this.getClass)


  /** 创建kudu表
   *
   * @param tableName    : 待创建表名
   * @param schemaMap    : 待创建表schema信息, Map[colName(String), (dataType(String), nullable(Boolean))] : ( Map["age", ("int", true)]
   * @param primaryKeys  : 主键, Seq[key(String)]
   * @param replicasNum  : 副本数
   * @param partitionNum : 分区数
   * @return 成功与否
   */
  def createTable(
                   tableName: String,
                   schemaMap: scala.collection.Map[String, (String, Boolean)],
                   primaryKeys: List[String],
                   replicasNum: Int = 1,
                   partitionNum: Int
                 ): Unit = {


    val structFields = schemaMap
      .map(r => {
        val colName = r._1
        val dataType = r._2._1 match {
          case "int" => IntegerType
          case "double" => DoubleType
          case "string" => StringType
          case "long" => LongType
          case "timestamp" => TimestampType
          case "binary" => BinaryType
          case "bool" => BooleanType
          case _ => StringType
        }
        val nullable = r._2._2

        StructField(colName, dataType, nullable)
      })
      .toList

    val schema = StructType(structFields)

    if (kuduContext.tableExists(tableName)) {
      throw new RuntimeException(tableName + ": table already exists")
    }

    try {
      kuduContext.createTable(tableName, schema, primaryKeys,
        new CreateTableOptions()
          //          .addHashPartitions(primaryKeys.asJava, partitionNum)
          .setNumReplicas(replicasNum))
      logger.info(s"create table '$tableName' suc")
    } catch {
      case e: Exception => throw new RuntimeException(s"creat table '$tableName' failed")
    }

  }


  /** spark以dataFrame读取kudu表
   *
   * @param tableName : 待读取kudu表
   * @param filterCmd : 过滤条件,  默认空
   * @return : DataFrame
   */
  def readDF(tableName: String, filterCmd: String = ""): DataFrame = {

    import ss.implicits._

    val df = ss.read.options(Map("kudu.master" -> kuduMaster, "kudu.table" -> tableName))
      .format("kudu").load

    df.createOrReplaceTempView("kuduTable")
    val table = ss.sql("select * from kuduTable")

    if (filterCmd == "") {
      table
    }
    else {
      table.filter(filterCmd)
    }
  }


  /** 插入表
   *
   * @param tableName : 待插入表名
   * @param df        : 待插入数据, DF格式
   * @return : 操作结果, boolean
   */
  def insert(tableName: String, df: DataFrame): Unit = {

    try {
      kuduContext.insertRows(df, tableName)

      //      df.write
      //        .options(Map("kudu.master" -> kuduMaster, "kudu.table" -> "test_table"))
      //        .mode("append")
      //        .format("kudu")
      //        .save

      logger.info(s"insert table '$tableName' suc")
    }
    catch {
      case e: Exception => throw new RuntimeException(s"insert table $tableName failed: " + e)
    }

  }


  /** upsert
   *
   * @param tableName  : 待操作表明
   * @param toUpsertDF : 待操作数据
   * @return : 操作结果, boolean
   */
  def upsert(tableName: String, toUpsertDF: DataFrame): Unit = {

    try {
      kuduContext.upsertRows(toUpsertDF, tableName)
      logger.info(s"upsert table '$tableName' suc")
    }
    catch {
      case e: Exception => throw new RuntimeException(s"upsert table $tableName failed: " + e)
    }
  }

  /** 更新表
   *
   * @param tableName  : 待更新表名
   * @param toUpdateDF : 待更新数据, DF格式
   * @return : 操作结果, boolean
   */
  def update(tableName: String, toUpdateDF: DataFrame): Unit = {

    try {
      kuduContext.updateRows(toUpdateDF, tableName)
      logger.info(s"update table '$tableName' suc")
    }
    catch {
      case e: Exception => throw new RuntimeException(s"update table $tableName failed: " + e)
    }
  }


  /** 删除表
   *
   * @param tableName : 待删除表名
   * @return : 操作结果, boolean
   */
  def deleteTable(tableName: String): Unit = {

    try {
      kuduContext.deleteTable(tableName)
      logger.info(s"delete table '$tableName' suc")
    }
    catch {
      case e: Exception => throw new RuntimeException(s"delete table $tableName failed:" + e)
    }
  }

  def kuduTable(spark: SparkSession, areaTable: String, areaView: String): Unit = {
    spark.read.format("kudu")
      .options(Map("kudu.master" -> "10.170.3.11,10.170.3.12,10.170.3.13",
        "kudu.table" -> areaTable))
      .load
      .createOrReplaceTempView(areaView)
  }

}

