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
package org.herry2038.scadb.admin

import java.util.concurrent.locks.ReentrantLock
import com.google.gson.{Gson}
import org.herry2038.scadb.admin.curator.{JobsManager, JobsListeners}
import org.herry2038.scadb.conf.ScadbConf
import org.herry2038.scadb.conf.jobs.JobsModel.JobResult
import org.herry2038.scadb.util.Log

import scala.collection.mutable

/**
 * Created by Administrator on 2016/3/9.
 */
class JobWorker(val business: String, val manager: JobsManager) extends Thread("JobWorker-" + business) {
  val log = Log.get[JobWorker]
  private val lock = new ReentrantLock()
  private val condition = lock.newCondition()
  private val zkPath = s"${ScadbConf.basePath}/jobs/${business}"
  private var isRunning = false
  private val q = mutable.Queue[(String,String)]()

  private val pathCache = ScadbConf.pathCache(zkPath, new JobsListeners.JobListener(this))


  def schedule(jobId: String, job: String) = {
    lock.lock
    try {
      q.enqueue((jobId, job))
      condition.signal()
    } finally {
      lock.unlock
    }
  }



  override def start(): Unit = {
    super.start()
  }

  def stopWork(): Unit = {
    isRunning = false
    this.interrupt()
  }

  override def run(): Unit = {
    setName(s"job-worker-${business}")
    log.info(s"worker for ${business} is running!!!")
    isRunning = true

    while ( isRunning ) {
      lock.lock()
      var jobId: String = null
      var job: String = null
      try {
        while (q.isEmpty && isRunning) {
          condition.await()
        }
        val jobInfo = q.dequeue()
        jobId = jobInfo._1
        job = jobInfo._2
        JobProcessor.process(business, jobId, job)
        jobFinished(jobId, job, 0, "success!")
      } catch {
        case e: InterruptedException =>
        case e: Throwable =>
          jobFinished(jobId, job, -1, e.getMessage)
          log.info(s"job running error ${jobId}", e)
      } finally {
        lock.unlock
      }
    }
  }

  def jobFinished(jobId: String, jobInfo: String, code: Int, msg: String) = {
    try {
      val jobResult = new JobResult(code, msg, business, jobInfo)
      val jsonString = new Gson().toJson(jobResult)

      //val result = s"""{"code":$code, "msg": "$msg", "business": "${business}"}"""
      log.info(s"job finished:$jobId, code:$code, msg:$msg")
      ScadbConf.client.delete().forPath(zkPath + "/" + jobId)
      try {
        ScadbConf.client.create().forPath(s"${ScadbConf.basePath}/finishedjobs/" + jobId, jsonString.getBytes())
      } catch {
        case e: Throwable =>
          log.error(s"job finished error:$jobId", e)
      }
    } catch {
      case e: Throwable =>
        log.error(s"job finished error:$jobId", e)
    }
  }

}