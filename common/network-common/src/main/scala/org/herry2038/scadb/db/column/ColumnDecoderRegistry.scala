
package org.herry2038.scadb.db.column

import java.nio.charset.Charset
import io.netty.buffer.ByteBuf
import org.herry2038.scadb.db.general.ColumnData

trait ColumnDecoderRegistry {

  def decode(kind: ColumnData, value: ByteBuf, charset : Charset) : Any

}
