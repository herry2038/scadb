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
package org.herry2038.scadb.conf.cluster

import org.herry2038.scadb.conf.ScadbConf
import org.herry2038.scadb.conf.cluster.ClusterModel.MySQLStatus
import com.google.gson.Gson
import org.apache.curator.framework.recipes.cache.PathChildrenCache
import ClusterConfListener._
import ClusterModel.MySQLStatus
import org.herry2038.scadb.util.Log
import scala.collection.concurrent.RDCSS_Descriptor
import scala.collection.mutable

class ClusterConf(val business: String, val cluster: String) {
  val log = Log.get[ClusterConf]

  val finalCluster = business + "." + cluster

  lazy val path = ClusterConf.path(business, cluster)
  val dataCache = ScadbConf.dataCache(path, new InstanceConfDataListener(this))
  var pathCache: PathChildrenCache = null

  val listeners = mutable.ArrayBuffer[ClusterConfListener]()

  def registerListener(proxyConfListener: ClusterConfListener) = {
    ScadbConf.wrapperWithLock { () =>
      listeners += proxyConfListener
    }
  }

  def unregisterListener(proxyConfListener: ClusterConfListener) = {
    ScadbConf.wrapperWithLock { () =>
      listeners -= proxyConfListener
    }
  }

  def uptNode() = {
    ScadbConf.wrapperWithLock { () =>
      if ( pathCache == null ) {
        pathCache = ScadbConf.pathCache(path, new InstanceConfPathListener(this))
      }

      val str = new String(dataCache.getCurrentData.getData(),"utf8")
      val clusterModel = new Gson().fromJson(str, classOf[ClusterModel.MasterModel])
      listeners.map(_.uptMasterModel(finalCluster, clusterModel))
    }
  }

  def delInstance(instance: String): Unit = {
    ScadbConf.wrapperWithLock { () =>
      listeners.map(_.delInstance(finalCluster, instance))
    }
  }

  def uptInstance(instance: String, s: String): Unit = {
    ScadbConf.wrapperWithLock { () =>
      val status = new Gson().fromJson(s, classOf[MySQLStatus])
      listeners.map(_.uptInstance(finalCluster, instance, status))
    }
  }

  def addInstance(instance: String, s: String): Unit = {
    ScadbConf.wrapperWithLock { () =>
      val status = new Gson().fromJson(s, classOf[MySQLStatus])
      listeners.map(_.addInstance(finalCluster, instance, status))
    }
  }

  def close(): Unit = {
    ScadbConf.wrapperWithLock { () =>
      Option(pathCache).map(_.close())
      dataCache.close()
      pathCache = null
    }
  }

}

object ClusterConf {
  def path(dbset: String, proxy: String) = ScadbConf.clustersPath + "/" + dbset + "/" + proxy
}
