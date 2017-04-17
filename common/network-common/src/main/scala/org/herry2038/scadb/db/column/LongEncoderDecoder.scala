
package org.herry2038.scadb.db.column

object LongEncoderDecoder extends ColumnEncoderDecoder {
  override def decode(value: String): Long = value.toLong
}