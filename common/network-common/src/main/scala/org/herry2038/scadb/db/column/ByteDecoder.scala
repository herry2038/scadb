package org.herry2038.scadb.db.column

object ByteDecoder extends ColumnDecoder {
  override def decode(value: String): Any = value.toByte
}
