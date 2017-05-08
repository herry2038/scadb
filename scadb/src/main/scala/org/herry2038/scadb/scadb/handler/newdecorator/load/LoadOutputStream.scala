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

import java.io.{File, OutputStream}

import org.apache.commons.io.FileUtils
import org.herry2038.scadb.scadb.handler.newdecorator.DecoratorInterface

/**
 * Created by Administrator on 2016/11/17.
 */
class LoadOutputStream(val filePrefix: String) {
  private var sequence = 1
  private var currentSize = 0

  var outputStream: OutputStream = {
    val file = new File(filePrefix + "_" + sequence)
    FileUtils.openOutputStream(file)
  }

  def write(b: Array[Byte], offset: Int, length: Int): Unit = {
    this.synchronized {
      outputStream.write(b, offset, length)
      currentSize += b.length
      if (currentSize >= DecoratorInterface.ONE_TIME_SIZE) {
        currentSize = 0
        sequence += 1
        close
        val file = new File(filePrefix + "_" + sequence)
        FileUtils.openOutputStream(file)
      }
    }
  }

  def close(): Unit = {
    try { outputStream.close } catch { case _:Exception => }
  }
}

