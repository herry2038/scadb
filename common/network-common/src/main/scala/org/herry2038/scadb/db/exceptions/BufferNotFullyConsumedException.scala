
package org.herry2038.scadb.db.exceptions

import io.netty.buffer.ByteBuf

class BufferNotFullyConsumedException ( buffer : ByteBuf )
  extends DatabaseException( "Buffer was not fully consumed by decoder, %s bytes to read".format(buffer.readableBytes()) )
