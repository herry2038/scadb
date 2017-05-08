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
package org.herry2038.scadb.scadb.handler.executor

import org.herry2038.scadb.mysql.codec.MessageTrack
import org.herry2038.scadb.mysql.server.{ResultCompactor, MultiResults, MySQLServerConnectionHandler}
import org.herry2038.scadb.scadb.conf.ScadbMySQLPools
import org.herry2038.scadb.scadb.server.processor.Statistics
import org.herry2038.scadb.util.Log

import scala.collection.JavaConversions._
import org.herry2038.scadb.db.QueryResult
import scala.util.Success
import scala.util.Failure
import org.herry2038.scadb.db.util.ExecutorServiceUtils.CachedExecutionContext

object ScadbStatementParallelMulti {
  val log = Log.get[ScadbStatementMulti]
}

class ScadbStatementParallelMulti(handler: MySQLServerConnectionHandler,
                                   val sqls: java.util.List[(String, String, MessageTrack)],
                                   val parallelDegree: Int, startTime: Long, requestType: Int,
                                   val isRead: Boolean, val resultComparactor: ResultCompactor = null) extends ScadbStatement(handler, startTime, requestType) {

  import ScadbStatementParallelMulti.log
  var affectedRow: Long = 0
  var err: Int = 0
  var errMsg: String = ""
  val results = if ( resultComparactor == null ) new MultiResults else new MultiResults(resultComparactor)

  var finisheds: Int = 0
  var currentPos: Int = 0

  def executeInternal(): Unit = {
    if ( err == 0 && currentPos < sqls.length ) {
      val (server, sql, track) = sqls.get(currentPos)
      currentPos += 1

      val pool = ScadbMySQLPools.getPool(server, isRead).getOrElse {
        log.error("cannot find server:{}", server)
        errorCallback(-1, String.format("cannot find pool:%s", server))
        return
      }

      pool.sendQuery(sql, track).onComplete {
        case Success(v) =>
          callback(v)
        case Failure(e) =>
          log.error("send query {} error: {} {} !", sql, e.getMessage, "")
          error(-1, e.getMessage)
      }
    }
  }

  def execute(): Int = {
    for ( i <- 0 until Math.min(sqls.size, parallelDegree) ) {
      this.synchronized {
        executeInternal()
      }
    }
    return 0
  }

  def callback(result: QueryResult) = {
    this.synchronized {
      finisheds += 1
      if (err == 0) {
        results.add(result)
        if (finisheds >= sqls.length) {
          Statistics.request(requestType, false, startTime)
          handler.write(results)
        } else {
          executeInternal()
        }
      }
    }
  }

  def errorCallback(errcode: Int, errmsg: String) = {
    this.synchronized {
      finisheds += 1
      if (err == 0) {
        err = errcode
        errMsg = errmsg
        Statistics.request(requestType, true, startTime)
        error(err, errMsg)
      }
    }
  }
}
