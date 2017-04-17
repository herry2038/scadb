
package org.herry2038.scadb.db.column

import org.joda.time.format.DateTimeFormat

object TimeWithTimezoneEncoderDecoder extends TimeEncoderDecoder {

  private val format = DateTimeFormat.forPattern("HH:mm:ss.SSSSSSZ")

  override def formatter = format

}
