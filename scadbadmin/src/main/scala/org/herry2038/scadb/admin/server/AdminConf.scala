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
package org.herry2038.scadb.admin.server

import java.util.concurrent.ConcurrentHashMap

import org.apache.curator.framework.recipes.leader.LeaderSelector
import org.apache.zookeeper.CreateMode
import org.herry2038.scadb.admin.ConnectionHelper
import org.herry2038.scadb.admin.curator.{JobsManager, JobsListeners}
import org.herry2038.scadb.admin.leader.{AdminLeaderListener, AdminMaster}
import org.herry2038.scadb.admin.model.BusinessWrapper
import org.herry2038.scadb.conf.ScadbConf._
import org.herry2038.scadb.conf._
import JobsListeners.StateChanged
import org.herry2038.scadb.util.{Config, Logging}
import scala.collection.JavaConversions._


object AdminConf extends ScadbConfManagerListener with ScadbConfListener with Logging{
  var zookeeper: String = null
  var node: String =  null
  var ip: String =    null
  var port: Int =     19527
  var parallelDegree = 10

  val businesses = new ConcurrentHashMap[String, BusinessWrapper]
  var manager: ScadbConfManager = _
  var jobsManager: JobsManager = _

  var leaderLatch: LeaderSelector = null

  def load(): Unit = {
    Config.loadConfig("/scadbadmin.properties")
    zookeeper = Config.instance.getString("zookeeper", "localhost:2181")
    node = Config.instance.getString("node", null)
    parallelDegree = Config.instance.getInt("parallelDegree", parallelDegree)

    if ( node == null || zookeeper == null ) {
      error("node or zookeeper is null")
      return
    }
    val ipAndPort = node.split(":")
    ip = ipAndPort(0)
    if ( ipAndPort.size >= 2 ) {
      port = ipAndPort(1).toInt
    }

    StateChangeListener.register(StateChanged)
    ScadbConf.start(zookeeper)

    manager = new ScadbConfManager()
    manager.registerListener(this)
    manager.start()
    jobsManager = new JobsManager

    leaderLatch = new LeaderSelector(ScadbConf.client, s"${ScadbConf.basePath}/admins/leader", new AdminLeaderListener) ;
    leaderLatch.autoRequeue() ;
    leaderLatch.start
  }

  override def updateBusiness(business: String, busiData: Business): Unit = Option(businesses.get(business)).foreach(_.uptBusiData(busiData))

  override def delBusiness(business: String): Unit = {
    businesses -= business
    ConnectionHelper.remove(business)
    AdminMaster.unassignOneBusiness(business)
  }

  override def addBusiness(business: String, busiData: Business): Unit = {
    businesses += (business -> new BusinessWrapper(business, busiData) )
    AdminMaster.assignOneBusiness(business)
  }

  override def addRule(business: String, rule: String, r: Rule): Unit = Option(businesses.get(business)).foreach(_.addRule(rule,r))

  override def delTable(business: String, table: String): Unit = Option(businesses.get(business)).foreach(_.delTable(table))

  override def uptTable(business: String, table: String, t: Table): Unit = Option(businesses.get(business)).foreach(_.uptTable(table, t))

  override def delRule(business: String, rule: String): Unit = Option(businesses.get(business)).foreach(_.delRule(rule))

  override def uptRule(business: String, rule: String, r: Rule): Unit = Option(businesses.get(business)).foreach(_.uptRule(rule,r))

  override def uptBusiness(business: String, busi: Business): Unit = {
    businesses.getOrElseUpdate(business, new BusinessWrapper(business)).uptBusiData(busi)
  }

  override def addTable(business: String, table: String, t: Table): Unit = Option(businesses.get(business)).foreach(_.addTable(table, t))

  override def addUser(business: String, user: String, u: User): Unit = None

  override def uptUser(business: String, user: String, u: User): Unit = None

  override def delUser(business: String, user: String): Unit = None

  override def uptWhitelist(business: String, whitelists: java.util.List[String]): Unit = None

  def createNode(): Boolean = {
    // 创建节点 ${basePath}/admins/nodes/${IP:PORT}
    try {
      client.create().withMode(CreateMode.EPHEMERAL).forPath(s"${ScadbConf.basePath}/admins/nodes/${node}")
      true
    } catch {
      case e: Throwable =>
        error(s"StateChanged create node ${ScadbConf.basePath}/admins/nodes/${node} error!", e)
        false
    }
  }
}
