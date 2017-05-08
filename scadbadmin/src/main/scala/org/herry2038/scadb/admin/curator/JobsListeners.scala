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

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.{PathChildrenCacheEvent, PathChildrenCacheListener}
import org.apache.curator.framework.state.{ConnectionState, ConnectionStateListener}
import org.apache.curator.utils.ZKPaths
import org.apache.zookeeper.CreateMode
import org.herry2038.scadb.admin.{ConnectionHelper, JobWorker}
import org.herry2038.scadb.admin.leader.AdminMaster
import org.herry2038.scadb.admin.server.AdminConf
import org.herry2038.scadb.conf.ScadbConf
import org.herry2038.scadb.util.Log

/**
 * Created by Administrator on 2016/3/9.
 */
object JobsListeners {
  val log = Log.get[JobListener]
  class JobListener(val jobWorker: JobWorker) extends PathChildrenCacheListener {
    override def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent): Unit = {
      event.getType match {
        case PathChildrenCacheEvent.Type.CHILD_ADDED =>
          val jobId = ZKPaths.getNodeFromPath(event.getData.getPath)
          log.info(s"get a new job $jobId")
          jobWorker.schedule(jobId, new String(event.getData.getData))
        case PathChildrenCacheEvent.Type.CHILD_UPDATED =>
          // ignore
        case PathChildrenCacheEvent.Type.CHILD_REMOVED =>
          // ignore
        case a: Any =>
          log.info("unhandled path node msg:{}", a)
      }
    }
  }

  object StateChanged extends ConnectionStateListener {
    var currentState = ConnectionState.LOST
    override def stateChanged(client: CuratorFramework, newState: ConnectionState): Unit = {
      if ( ( newState == ConnectionState.CONNECTED || newState == ConnectionState.RECONNECTED ) && currentState == ConnectionState.LOST ) {
        // 创建节点 ${ScadbConf.basePath}/admins/nodes/${IP:PORT}
        try {
          client.create().withMode(CreateMode.EPHEMERAL).forPath(s"${ScadbConf.basePath}/admins/nodes/${AdminConf.node}")
        } catch {
          case e: Throwable =>
            log.error(s"StateChanged create node ${ScadbConf.basePath}/admins/nodes/${AdminConf.node} error!", e)
            System.exit(-1)
        }
      }
      currentState = newState
    }
  }

  class NodesListener extends PathChildrenCacheListener {
    override def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent): Unit = {
      event.getType match {
        case PathChildrenCacheEvent.Type.CHILD_ADDED =>
          val node = ZKPaths.getNodeFromPath(event.getData.getPath)
          log.info(s"get a new node: $node")
          AdminMaster.addNode(node)
        case PathChildrenCacheEvent.Type.CHILD_UPDATED =>
        // ignore
        case PathChildrenCacheEvent.Type.CHILD_REMOVED =>
          val node = ZKPaths.getNodeFromPath(event.getData.getPath)
          log.info(s"a node died: $node")
          AdminMaster.removeNode(node)
          ConnectionHelper.remove(node)
        case a: Any =>
          log.info("unhandled path node msg:{}", a)
      }
    }
  }

  class NodeAssignsListener extends PathChildrenCacheListener {
    override def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent): Unit = {
      event.getType match {
        case PathChildrenCacheEvent.Type.CHILD_ADDED =>
          val node = ZKPaths.getNodeFromPath(event.getData.getPath)
          log.info(s"get a new node: $node")
          AdminMaster.addNodeAssign(node, event.getData.getData)
        case PathChildrenCacheEvent.Type.CHILD_UPDATED =>
          val node = ZKPaths.getNodeFromPath(event.getData.getPath)
          AdminMaster.uptNodeAssign(node, event.getData.getData)
        case PathChildrenCacheEvent.Type.CHILD_REMOVED =>
        case a: Any =>
          log.info("unhandled path node msg:{}", a)
      }
    }
  }
}
