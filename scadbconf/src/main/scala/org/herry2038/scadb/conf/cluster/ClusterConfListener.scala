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

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.{PathChildrenCacheEvent, PathChildrenCacheListener, PathChildrenCache, NodeCacheListener}
import ClusterModel._
import org.apache.curator.utils.ZKPaths
import org.herry2038.scadb.util.Log

/**
 * Created by Administrator on 2016/2/26.
 */

trait ClusterConfListener {
  def delInstance(proxy: String, instance: String): Unit

  def addInstance(proxy: String, instance: String, status: MySQLStatus): Unit

  def uptInstance(proxy: String, instance: String, status: MySQLStatus): Unit

  def uptMasterModel(proxy: String, cluterModel: MasterModel): Unit
}


object ClusterConfListener {
  val log = Log.get[ClusterConfListener]

  class InstanceConfDataListener(val proxy: ClusterConf) extends NodeCacheListener {
    val log = Log.get[InstanceConfDataListener]

    override def nodeChanged(): Unit = {
      log.info("business data listener changed!")
      proxy.uptNode()
    }
  }

  class InstanceConfPathListener(val proxy: ClusterConf) extends PathChildrenCacheListener {
    override def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent): Unit = {
      event.getType match {
        case PathChildrenCacheEvent.Type.CHILD_ADDED =>
          val instance = ZKPaths.getNodeFromPath(event.getData.getPath)
          proxy.addInstance(instance, new String(event.getData.getData))
        case PathChildrenCacheEvent.Type.CHILD_UPDATED =>
          val instance = ZKPaths.getNodeFromPath(event.getData.getPath)
          proxy.uptInstance(instance, new String(event.getData.getData))
        case PathChildrenCacheEvent.Type.CHILD_REMOVED =>
          val instance = ZKPaths.getNodeFromPath(event.getData.getPath)
          proxy.delInstance(instance)
        case a: Any =>
          log.info("unhandled path node msg:{}", a)
      }
    }
  }

}
