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
package org.herry2038.scadb.admin.curator

import org.herry2038.scadb.admin.JobWorker
import org.herry2038.scadb.util.Log

import scala.collection.mutable

class JobsManager  {
  val log = Log.get[JobsManager]

  val businesses = new mutable.HashMap[String, JobWorker] with mutable.SynchronizedMap[String, JobWorker]

  def assignBusiness(business: String): (Boolean, String) = {
    try {
      if ( businesses.contains(business)) {
        return (false, s"business ${business} already assign!!!")
      }
      val worker = new JobWorker(business, this)
      businesses += (business -> worker)
      worker.start()
      (true, null)
    } catch {
      case e: Throwable =>
        log.error(s"assign business: ${business} error", e)
        (false, e.getMessage)
    }
  }

  def unassignBusiness(business: String): Boolean = {
    val jobWorker = businesses.remove(business)
    jobWorker.map { worker =>
      worker.stopWork()
      true
    }.getOrElse(false)
  }

}
