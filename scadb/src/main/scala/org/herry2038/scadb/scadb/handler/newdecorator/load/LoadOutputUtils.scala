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

import org.herry2038.scadb.conf.algorithm.Algorithm

import scala.collection.mutable.ArrayBuffer

class LoadOutputUtils(val algorithm: Algorithm, val dir: String, val table: String) {
  private val outputs = {
    val allOutputs = ArrayBuffer[LoadOutputStream]()
    for ( i <- 0 until algorithm.partitionNum ) {
      allOutputs += new LoadOutputStream(dir + "/loading_" + table + "_" + i)
    }
    allOutputs
  }

  def put(key: String, line: Array[Byte]): Unit = {
    val (_, partitionNo) = algorithm.partition(key)
    outputs(partitionNo).write(line, 0, line.length)
  }

  def put(key: String, line: Array[Byte], offset: Int, length: Int): Unit = {
    val (_, partitionNo) = algorithm.partition(key)
    outputs(partitionNo).write(line, offset, length)
  }

  def close(): Unit = {
    outputs.foreach(_.close)
    outputs.clear
  }
}

