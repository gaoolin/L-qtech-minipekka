package cn.qtech.minipekka.utils

import com.typesafe.config.ConfigFactory
import org.apache.spark.rdd.RDD
import play.api.libs.mailer._

/**
 * author   :  jdi146
 * contact  :  jdi147.com@gmail.com
 * date     :  2021/4/30 15:43
 */


object SendMail {


  def createMailer(host: String, port: Int, user: String, password: String, timeout: Int = 100000, connectionTimeout: Int = 1000000): SMTPMailer = {
    val configuration = new SMTPConfiguration(
      host, port, false, false, false,
      Option(user), Option(password), false, timeout = Option(timeout),
      connectionTimeout = Option(connectionTimeout), ConfigFactory.empty(), false
    )
    val mailer: SMTPMailer = new SMTPMailer(configuration)
    mailer
  }

  def createEmail(
                   subject: String,
                   from: String,
                   to: Seq[String],
                   bodyText: String = "ok",
                   bodyHtml: String = "",
                   charset: String = "gbk",
                   attachments: Seq[Attachment] = Seq.empty
                 ):
  Email = {

    val email = Email(subject, from, to,
      bodyText = Option[String](bodyText),
      charset = Option[String](charset),
      bodyHtml = Option[String](bodyHtml)
      , attachments = attachments
    )
    email
  }


  def createAttachment(attachmentName: String, rawData: RDD[(String, String, String, String, String, String)], mimetype: String = "text/csv")
  : Attachment = {
    val data: Array[Byte] = rawData.collect().mkString("\n").getBytes("gbk")
    val attachment = AttachmentData(attachmentName, data, mimetype)
    attachment
  }

  // 测试用， MailTest
  def createAttachment1(attachmentName: String, rawData: RDD[(String, String, String, Int, String)], mimetype: String = "text/csv")
  : Attachment = {
    val data: Array[Byte] = rawData.collect().mkString("\n").getBytes("gbk")
    val attachment = AttachmentData(attachmentName, data, mimetype)
    attachment
  }

}
