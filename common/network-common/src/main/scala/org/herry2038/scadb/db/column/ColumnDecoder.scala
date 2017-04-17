
package org.herry2038.scadb.db.column

import java.nio.charset.Charset
import io.netty.buffer.ByteBuf
import org.herry2038.scadb.db.general.ColumnData

trait ColumnDecoder {

  def decode( kind : ColumnData, value : ByteBuf, charset : Charset ) : Any = {
    val bytes = new Array[Byte](value.readableBytes())
    value.readBytes(bytes)
    decode(new String(bytes, charset))
  }

  def decode( value : String ) : Any

  def supportsStringDecoding : Boolean = true

}
