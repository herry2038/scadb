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
package org.herry2038.scadb.scadb.handler.ddl

import java.util.Date
import org.herry2038.scadb.conf.ScadbConf
import org.herry2038.scadb.conf.jobs.JobsModel.JobResult
import org.herry2038.scadb.conf.jobs.{JobsModel, JobResultsListener}
import org.herry2038.scadb.conf.jobs.JobResultsListener
import org.herry2038.scadb.util.Logging

import scala.concurrent.Promise
import scala.collection.mutable
import scala.util.Try

object DdlJobsListener extends JobResultsListener with Logging{
  class DdlJob(val startTime: Date, val promise: Promise[JobResult])

  val map = mutable.HashMap[String, DdlJob]()
  override def deleteFinishedJob(jobId: String): Unit = None

  override def finishJob(jobId: String, jobResult: JobResult): Unit = {
    this.synchronized {
      info(s"ddljob finished:${jobId}, result:${jobResult.msg}")
      map.get(jobId).map{ job =>
        info(s"ddljob real finished:${jobId}, result:${jobResult.msg}")
        job.promise.success(jobResult)
        Try(ScadbConf.client.delete().forPath(s"${ScadbConf.basePath}/finishedjobs/${jobId}"))
      }
      map -= jobId
    }
  }

  def startJob(jobId: String, promise: Promise[JobResult]): Unit = {
    this.synchronized {
      map += (jobId -> new DdlJob(new Date(), promise))
    }
  }

  def handleTimeoutJobs(): Unit = {
    val now = new Date() ;
    this.synchronized {
      val filtered = map.filter(now.getTime() - _._2.startTime.getTime >= 7200000 )

      filtered.map{
        case (jobId: String, job: DdlJob) =>
          info(s"ddljob:${jobId} timeouted!!!!")
          job.promise.success(new JobResult(-1,"time outed!", null,null))
          map -= jobId
      }
    }
  }
}
