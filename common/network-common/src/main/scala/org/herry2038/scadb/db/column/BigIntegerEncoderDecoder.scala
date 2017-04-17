
package org.herry2038.scadb.db.column

object BigIntegerEncoderDecoder extends ColumnEncoderDecoder {
  override def decode(value: String): Any = BigInt(value)
}
