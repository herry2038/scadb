
package org.herry2038.scadb.db.column

import org.joda.time.LocalTime
import org.joda.time.format.DateTimeFormatterBuilder

object TimeEncoderDecoder {
  val Instance = new TimeEncoderDecoder()
}

class TimeEncoderDecoder extends ColumnEncoderDecoder {

  final private val optional = new DateTimeFormatterBuilder()
    .appendPattern(".SSSSSS").toParser

  final private val format = new DateTimeFormatterBuilder()
    .appendPattern("HH:mm:ss")
    .appendOptional(optional)
    .toFormatter

  def formatter = format

  override def decode(value: String): LocalTime = {
    format.parseLocalTime(value)
  }

  override def encode(value: Any): String = {
    this.format.print(value.asInstanceOf[LocalTime])
  }

}
