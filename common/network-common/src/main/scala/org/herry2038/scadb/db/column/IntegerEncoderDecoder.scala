
package org.herry2038.scadb.db.column

object IntegerEncoderDecoder extends ColumnEncoderDecoder {

  override def decode(value: String): Int = value.toInt

}
