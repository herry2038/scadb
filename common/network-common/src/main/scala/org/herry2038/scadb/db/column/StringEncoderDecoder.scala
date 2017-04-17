
package org.herry2038.scadb.db.column

object StringEncoderDecoder extends ColumnEncoderDecoder {
  override def decode(value: String): String = value
}
