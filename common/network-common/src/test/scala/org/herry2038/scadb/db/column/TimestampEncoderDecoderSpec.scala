
package org.herry2038.scadb.db.column

import org.specs2.mutable.Specification
import org.joda.time.DateTime
import java.sql.Timestamp
import org.joda.time.format.DateTimeFormatterBuilder
import java.util.Calendar

class TimestampEncoderDecoderSpec extends Specification {

  val encoder = TimestampEncoderDecoder.Instance
  val dateTime = new DateTime()
    .withDate(2013, 12, 27)
    .withTime(8, 40, 50, 800)

  val result = "2013-12-27 08:40:50.800000"
  val formatter = new DateTimeFormatterBuilder().appendPattern("Z").toFormatter
  val resultWithTimezone = s"2013-12-27 08:40:50.800000${formatter.print(dateTime)}"

  "decoder" should {

    "should print a timestamp" in {
      val timestamp = new Timestamp(dateTime.toDate.getTime)
      encoder.encode(timestamp) === resultWithTimezone
    }

    "should print a LocalDateTime" in {
      encoder.encode(dateTime.toLocalDateTime) === result
    }

    "should print a date" in {
      encoder.encode(dateTime.toDate) === resultWithTimezone
    }

    "should print a calendar" in {
      val calendar = Calendar.getInstance()
      calendar.setTime(dateTime.toDate)
      encoder.encode(calendar) === resultWithTimezone
    }

    "should print a datetime" in {
      encoder.encode(dateTime) === resultWithTimezone
    }

  }

}