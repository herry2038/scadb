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
package org.herry2038.scadb.mysql.message.server

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import io.netty.buffer.ByteBuf

class ResultSetRowMessage
  extends ServerMessage( ServerMessage.Row )
  with mutable.Buffer[ByteBuf]
{

  private val buffer = new ArrayBuffer[ByteBuf]()

  def length: Int = buffer.length

  def apply(idx: Int): ByteBuf = buffer(idx)

  def update(n: Int, newelem: ByteBuf) {
    buffer.update(n, newelem)
  }

  def +=(elem: ByteBuf): this.type = {
    this.buffer += elem
    this
  }

  def clear() {
    this.buffer.clear()
  }

  def +=:(elem: ByteBuf): this.type = {
    this.buffer.+=:(elem)
    this
  }

  def insertAll(n: Int, elems: Traversable[ByteBuf]) {
    this.buffer.insertAll(n, elems)
  }

  def remove(n: Int): ByteBuf = {
    this.buffer.remove(n)
  }

  override def iterator: Iterator[ByteBuf] = this.buffer.iterator

}