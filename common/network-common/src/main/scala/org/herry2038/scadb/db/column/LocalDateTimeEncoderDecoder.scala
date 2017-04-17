
package org.herry2038.scadb.db.column

import org.joda.time.format.DateTimeFormatterBuilder
import org.joda.time.LocalDateTime

object LocalDateTimeEncoderDecoder extends ColumnEncoderDecoder {

  private val ZeroedTimestamp = "0000-00-00 00:00:00"

  private val optional = new DateTimeFormatterBuilder()
    .appendPattern(".SSSSSS").toParser

  private val format = new DateTimeFormatterBuilder()
    .appendPattern("yyyy-MM-dd HH:mm:ss")
    .appendOptional(optional)
    .toFormatter

  override def encode(value: Any): String =
    format.print(value.asInstanceOf[LocalDateTime])

  override def decode(value: String): LocalDateTime =
    if (ZeroedTimestamp == value) {
      null
    } else {
      format.parseLocalDateTime(value)
    }

}
