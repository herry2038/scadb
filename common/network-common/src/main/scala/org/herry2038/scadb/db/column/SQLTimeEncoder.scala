
package org.herry2038.scadb.db.column

import org.joda.time.format.DateTimeFormatterBuilder
import org.joda.time.LocalTime

object SQLTimeEncoder extends ColumnEncoder {

  final private val format = new DateTimeFormatterBuilder()
    .appendPattern("HH:mm:ss")
    .toFormatter

  override def encode(value: Any): String = {
    val time = value.asInstanceOf[java.sql.Time]

    format.print( new LocalTime(time.getTime) )
  }
}
