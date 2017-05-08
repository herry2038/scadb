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
package org.herry2038.scadb.scadb.server.processor

import java.util.concurrent.atomic.AtomicLong

import org.herry2038.scadb.util.Log

/**
 * Created by Administrator on 2017/3/29.
 */

class Statistics {

  val opts: Array[Array[AtomicLong]] = Array(
    Array(new AtomicLong(0), new AtomicLong(0), new AtomicLong(0)),
    Array(new AtomicLong(0), new AtomicLong(0), new AtomicLong(0)),
    Array(new AtomicLong(0), new AtomicLong(0), new AtomicLong(0)),
    Array(new AtomicLong(0), new AtomicLong(0), new AtomicLong(0)),
    Array(new AtomicLong(0), new AtomicLong(0), new AtomicLong(0))
  )

  def request(requestType: Int, isError: Boolean, executeTime: Long): Unit = {
    opts(requestType)(0).addAndGet(1)
    opts(requestType)(1).addAndGet(executeTime)
    if ( isError )
      opts(requestType)(2).addAndGet(1)
  }

  def log(total: Long): Unit = {
    Statistics.log.warn(s"statistics_metrics ${total}: ${opts(0)(0)},${opts(0)(1)},${opts(0)(2)}  ${opts(1)(0)},${opts(1)(1)},${opts(1)(2)}  ${opts(2)(0)},${opts(2)(1)},${opts(2)(2)}  ${opts(3)(0)},${opts(3)(1)},${opts(3)(2)}  ${opts(4)(0)},${opts(4)(1)},${opts(4)(2)}")
    opts.foreach(opt=>opt.foreach(_.set(0)))
  }
}


object Statistics {
  val log = Log.get[Statistics]

  val SELECT_TYPE = 0
  val INSERT_TYPE = 1
  val UPDATE_TYPE = 2
  val DELETE_TYPE = 3
  val REPLACE_TYPE = 4
  val OTHER_TYPE = -1


  val requestCounter: AtomicLong = new AtomicLong(0)
  var lastTimeRequestCounter: Long = 0

  var statisticsDataPos = 0
  val statisticsData : Array[Statistics] = Array(new Statistics(),new Statistics())

  def record(): Unit = {
    val currentPos = statisticsDataPos
    statisticsDataPos = if ( currentPos == 0 ) 1 else 0
    val currentCounter = requestCounter.get

    val logCounter = currentCounter - lastTimeRequestCounter
    lastTimeRequestCounter = currentCounter
    statisticsData(currentPos).log(logCounter)
  }

  def counter() = requestCounter.addAndGet(1)

  def request(requestType: Int, isError: Boolean, startTime: Long): Unit = {
    try {
      if ( requestType != OTHER_TYPE )
        statisticsData(statisticsDataPos).request(requestType, isError, (System.nanoTime() - startTime)/1000 )
    }catch {
      case _: Throwable =>
    }
  }
}
