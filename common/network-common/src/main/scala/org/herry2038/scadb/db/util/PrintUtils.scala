
package org.herry2038.scadb.db.util

import io.netty.buffer.ByteBuf

object PrintUtils {

  private val log = Log.getByName(this.getClass.getName)

  def printArray( name : String, buffer : ByteBuf ) {
    buffer.markReaderIndex()
    val bytes = new Array[Byte](buffer.readableBytes())
    buffer.readBytes(bytes)
    buffer.resetReaderIndex()
    log.debug( s"$name Array[Byte](${bytes.mkString(", ")})" )
  }

}
