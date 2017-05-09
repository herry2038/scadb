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
package org.herry2038.scadb.conf.jobs

import com.google.gson.Gson
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.{PathChildrenCacheEvent, PathChildrenCacheListener}
import org.apache.curator.utils.ZKPaths
import org.herry2038.scadb.conf.ScadbConf
import org.herry2038.scadb.conf.business.ScadbConfBusiness
import org.herry2038.scadb.util.Log


object ScadbConfJobsListener {
  class ScadbConfJobTaskListener(val listener: JobsListener ) extends PathChildrenCacheListener {
    val log = Log.get[ScadbConfJobTaskListener]

    override def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent): Unit = {
      event.getType match {
        case PathChildrenCacheEvent.Type.CHILD_ADDED =>
          val jobId = ZKPaths.getNodeFromPath(event.getData.getPath)
          val jobInfo = new Gson().fromJson(new String(event.getData().getData()), classOf[JobsModel.JobInfo])

          ScadbConf.wrapperWithLock { () =>
            listener.newJob(jobId, jobInfo)
          }

        case PathChildrenCacheEvent.Type.CHILD_UPDATED =>
        case PathChildrenCacheEvent.Type.CHILD_REMOVED =>
          val jobId = ZKPaths.getNodeFromPath(event.getData.getPath)
          ScadbConf.wrapperWithLock { () =>
            listener.deleteJob(jobId)
          }
        case a: Any =>
          log.info("unhandled path node msg:{}", a)
      }
    }
  }


  class ScadbConfJobResultListener(val listener: JobResultsListener ) extends PathChildrenCacheListener {
    val log = Log.get[ScadbConfJobResultListener]

    override def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent): Unit = {
      event.getType match {
        case PathChildrenCacheEvent.Type.CHILD_ADDED =>
          val jobId = ZKPaths.getNodeFromPath(event.getData.getPath)
          val jobResult = new Gson().fromJson(new String(event.getData().getData()), classOf[JobsModel.JobResult])

          ScadbConf.wrapperWithLock { () =>
            listener.finishJob(jobId, jobResult)
          }

        case PathChildrenCacheEvent.Type.CHILD_UPDATED =>
        case PathChildrenCacheEvent.Type.CHILD_REMOVED =>
          val jobId = ZKPaths.getNodeFromPath(event.getData.getPath)
          ScadbConf.wrapperWithLock { () =>
            listener.deleteFinishedJob(jobId)
          }
        case a: Any =>
          log.info("unhandled path node msg:{}", a)
      }
    }
  }
}
