
package org.herry2038.scadb.db.column

object FloatEncoderDecoder extends ColumnEncoderDecoder {
  override def decode(value: String): Float = value.toFloat
}
