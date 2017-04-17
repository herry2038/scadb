
package org.herry2038.scadb.db.column

object BigDecimalEncoderDecoder extends ColumnEncoderDecoder {

  override def decode(value: String): Any = BigDecimal(value)

}
