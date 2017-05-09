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

import java.util.{UUID, Date}

import org.herry2038.scadb.conf.ScadbConf
import org.herry2038.scadb.conf.jobs.JobsModel.JobResult
import org.herry2038.scadb.mysql.MySQLQueryResult
import org.herry2038.scadb.db.util.{DateUtils, ExecutorServiceUtils}
import org.herry2038.scadb.mysql.server.MySQLServerConnectionHandler
import org.herry2038.scadb.scadb.conf.ScadbConfig
import org.herry2038.scadb.scadb.handler.ddl.DdlJobsListener
import org.herry2038.scadb.util.Log

import scala.concurrent.Promise

class ScadbStatementDdl(handler: MySQLServerConnectionHandler,val sql:String) extends ScadbStatement(handler, 0, -1) {
  val log = Log.get[ScadbStatementDdl]

  // 是否
  implicit val executeContext = ExecutorServiceUtils.CachedExecutionContext

  override def execute(): Int = {
    val job = s"${ScadbConf.basePath}/jobs/${ScadbConfig.conf.business}/${jobId}"

    val promise = Promise[JobResult]()
    val f = promise.future

    log.info(s"start ddljob:${jobId} sql:\n${sql}")

    DdlJobsListener.startJob(jobId, promise)
    ScadbConf.client.create().forPath(job, sql.getBytes())

    f.map { result =>
      if ( result.code == 0 ) {
        val ok = new MySQLQueryResult(0, null, 0, 0, 0)
        write(ok)
      } else {
        log.error(s"send query $sql error: ${result.code} ${result.msg} !")
        error(result.code,result.msg)
      }
    }
    0
  }

  val jobId = {
    val buffer = new StringBuffer()
    buffer.append(DateUtils.format(new Date(), "yyyyMMddHHmmss"))
    buffer.append(':').append(ScadbConfig.conf.business)
    buffer.append(':').append(UUID.randomUUID())
    buffer.toString()
  }
}
