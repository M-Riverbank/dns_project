package dsy.model

import scala.util.Random
import java.time._

object time {
  def main(args: Array[String]): Unit = {
    val time: Long =
      System.currentTimeMillis() / 1000 - 1563465600
//      System.currentTimeMillis() / 1000 - new Random().nextInt(15552000)
    val instant = Instant.ofEpochSecond(time)
    val localDateTime = LocalDateTime.ofInstant(instant, ZoneOffset.UTC)

    val year = localDateTime.getYear
    val month = localDateTime.getMonthValue
    val dayOfMonth = localDateTime.getDayOfMonth
    val hour = localDateTime.getHour
    val minute = localDateTime.getMinute
    val second = localDateTime.getSecond

    println(s"$year/$month/$dayOfMonth $hour:$minute:$second")
  }
}
