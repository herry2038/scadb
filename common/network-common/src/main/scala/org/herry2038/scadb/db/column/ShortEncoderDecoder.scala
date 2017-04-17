
package org.herry2038.scadb.db.column

object ShortEncoderDecoder extends ColumnEncoderDecoder {

  override def decode(value: String): Any = value.toShort

}