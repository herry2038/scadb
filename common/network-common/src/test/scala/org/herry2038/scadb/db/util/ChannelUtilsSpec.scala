package org.herry2038.scadb.db.util

import org.specs2.mutable.Specification
import io.netty.util.CharsetUtil
import io.netty.buffer.Unpooled


class ChannelUtilsSpec extends Specification {

  val charset = CharsetUtil.UTF_8

  "utils" should {

    "correctly write and read a string" in {
      val content = "some text"
      val buffer = Unpooled.buffer()

      ByteBufferUtils.writeCString(content, buffer, charset)

      ByteBufferUtils.readCString(buffer, charset) === content
      buffer.readableBytes() === 0
    }

    "correctly read the buggy MySQL EOF string when there is an EOF" in {
      val content = "some text"
      val buffer = Unpooled.buffer()

      ByteBufferUtils.writeCString(content, buffer, charset)

      ByteBufferUtils.readUntilEOF(buffer, charset) === content
      buffer.readableBytes() === 0
    }

    "correctly read the buggy MySQL EOF string when there is no EOF" in {

      val content = "some text"
      val buffer = Unpooled.buffer()

      buffer.writeBytes(content.getBytes(charset))

      ByteBufferUtils.readUntilEOF(buffer, charset) === content
      buffer.readableBytes() === 0

    }

  }

}
