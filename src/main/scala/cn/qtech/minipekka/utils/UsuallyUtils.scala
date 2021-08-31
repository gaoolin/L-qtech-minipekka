package cn.qtech.minipekka.utils

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

object UsuallyUtils {


  def getTimestamp(format: String = "yyyy-MM-dd"): String = {

    val df = new SimpleDateFormat(format)
    val calendar = Calendar.getInstance()

//    calendar.add(Calendar.DAY_OF_MONTH, -1)
//    val res = df.format(calendar.getTime)
    val date = df.format(new Date())
     val res = date + " 00:00:00"
    res

  }

}
