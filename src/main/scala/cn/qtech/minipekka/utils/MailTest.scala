package cn.qtech.minipekka.utils

import org.apache.spark.sql.SparkSession
import play.api.libs.mailer.{Attachment, Email, SMTPMailer}

/**
 * author   :  jdi146
 * contact  :  jdi147.com@gmail.com
 * date     :  2021/4/30 16:56
 */
object MailTest {


  def main(args: Array[String]): Unit = {

    /** -------------------------------------------------- 【附件创建】 ------------------------------------------------------- */
    val name = "附件"
    val ss: SparkSession = SparkSession.builder().appName("test").master("local[*]").getOrCreate()
    val df = ss.createDataFrame(
      Seq(
        ("864999046743439", "C0HF34#1", "2021-04-23 16:57:39", 0, "qualified"),
        ("864999046743439", "C0HF34#2", "2021-04-23 22:57:39", 1, "|lineNmb11_leadOffset_7.22_padOffset_0.10|lineNmb12_leadOffset_5.88_padOffset_0.65|"),
        ("864999046743429", "COAD #6", "2021-04-23 16:57:39", 2, "少线: 标准/实际: 80/84")
      )
    )
      .toDF("equipmentID", "mcID", "datetime", "code", "description")
    df.show(5, truncate = false)
    val data = df.rdd.map(r => (r.getAs[String]("equipmentID"), r.getAs[String]("mcID"),
      r.getAs[String]("datetime"), r.getAs[Int]("code"), r.getAs[String]("description")))
        val attachment: Attachment = SendMail.createAttachment1(name, rawData = data)


    /** ------------------------------------------------------ 【send mail】------------------------------------------------------------ */

    val subject = "the test of sending mail"
    val from = "bigdata.it@qtechglobal.com"
    val to = Seq("limeng.gu@qtechglobal.com")
    val host = "123.58.177.49"
    val port = 25
    val user = "bigdata.it@qtechglobal.com"
    val password = "qtech2020"
    val myMailer: SMTPMailer = SendMail.createMailer(host, port, user, password)
    val emails: Email = SendMail.createEmail(subject, from, to, attachments = Seq(attachment)) // , attachments = Seq(attachment)

    val code = myMailer.send(emails)
    println(s"---------------------------------------code: $code ----------------------------------------------")
  }

}
