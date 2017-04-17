
package org.herry2038.scadb.db.column

object DoubleEncoderDecoder extends ColumnEncoderDecoder {
  override def decode(value: String): Double = value.toDouble
}
