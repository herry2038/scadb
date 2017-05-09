//=========================================================================\\
//     _____               _ _
//    / ____|             | | |
//   | (___   ___ __ _  __| | |__
//    \___ \ / __/ _` |/ _` | '_ \
//    ____) | (_| (_| | (_| | |_) |
//   |_____/ \___\__,_|\__,_|_.__/

// Copyright 2016 The Scadb Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.
//=========================================================================\\
package org.herry2038.scadb.scadb.handler.newdecorator.load

import java.io.{File, FileInputStream}
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicLong
import org.apache.commons.io.IOUtils
import org.herry2038.scadb.scadb.handler.exceptions.ExecuteException
import org.herry2038.scadb.util.Log


object LoadUtils {
  class LoadUtilAnonymous
  val log = Log.get[LoadUtilAnonymous]
  def handle(file: File, offset: Int, length: Int, keyPos: Int, output: LoadOutputUtils): Long = {
    val data = new Array[Byte](length.toInt)
    var inputStream: FileInputStream = null
    try {
      inputStream = new FileInputStream(file)
      val readLength = IOUtils.read(inputStream, data, offset, length)
      assert(readLength == length)
      var begin = 0
      var end = 0

      var lines: Long = 0
      while (begin < data.length ) {
        val (key,pos) = findKey(data, begin, '"', keyPos)
        if ( key == null ) {
          log.error(s"load data for file ${file.getAbsolutePath} position: ${offset + begin} cannot find valid key!!!")
          throw new ExecuteException("data format invalid ,cannot get key!")
        }
        end = pos
        while ( data(end) != '\n' ) end += 1
        output.put(key, data, begin, end - begin + 1)
        lines += 1
        begin = end + 1
      }
      lines
    } finally {
      if (inputStream != null) inputStream.close
    }
  }

  def findKey(data: Array[Byte], begin: Int, encloseChar: Byte, keyPos: Int): (String, Int) = {
    var current = begin
    var colPos = begin
    var inStr = false
    var currentColPos = 0

    var keyBeginPosition = current
    while ( current < data.length && data(current) != '\n' ) {
      val ch = data(current)
      if ( ch == ',' ) {
        if (!inStr) {
          if (currentColPos == keyPos) {
            return (new String(data, keyBeginPosition, current - keyBeginPosition, "utf-8"), current + 1)
          } else {
            currentColPos += 1
          }
        }
      } else if ( ch == encloseChar ) {
        if (!inStr) {
          inStr = true
          if ( currentColPos == keyPos ) keyBeginPosition = current
        } else {
          inStr = false
          if ( currentColPos == keyPos ) {
            return (new String(data, keyBeginPosition, current - keyBeginPosition, "utf-8"), current + 1)
          }
        }
      } else if ( ch == '\\' ) {
        if (inStr) {
          current += 1
          if (current >= data.length)
            return (null, -1)
        } else if (data(current) != 'N') {
          return (null, -1)
        }
      }
      current += 1
    }

    if ( current < data.length )
      current += 1
    if ( currentColPos == keyPos ) {
      return (new String(data, keyBeginPosition, current - keyBeginPosition, "utf-8"), current)
    }
    return (null, current)
  }
}
