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

package org.herry2038.scadb.sentinel

import java.util.concurrent.ConcurrentHashMap
import org.apache.curator.framework.recipes.leader.LeaderSelector
import org.herry2038.scadb.conf.ScadbConf
import org.herry2038.scadb.conf.cluster.ClusterConfListener
import org.herry2038.scadb.conf.cluster.ClusterModel.{MySQLStatus, MasterModel}
import org.herry2038.scadb.conf.utils.ZKUtils
import org.herry2038.scadb.switcher.{MysqlSwitcher, SentinelAutoSwitch}
import org.herry2038.scadb.util.Logging

import scala.util.Try

object SentinelAutoswitchService extends ClusterConfListener with Logging {
  private var leaderLatch: LeaderSelector = null

  var switcher: MysqlSwitcher = null
  val clusters = new ConcurrentHashMap[String, SentinelAutoSwitch]()

  var started = false

  def start = {
    Try(ScadbConf.client.setData.forPath("/scadb/sentinel/currentleader", SentinelConf.zoneId.getBytes))
    started = true
  }
  def stop = started = false


  def startLeaderSelector(): Unit = {
    Try(ZKUtils.create("/scadb/sentinel"))
    Try(ZKUtils.create("/scadb/sentinel/leader"))
    Try(ZKUtils.create("/scadb/sentinel/currentleader"))

    leaderLatch = new LeaderSelector(ScadbConf.client, "/scadb/sentinel/leader", new SentinelLeaderListener)
    leaderLatch.autoRequeue()
    leaderLatch.start
  }


  override def delInstance(cluster: String, instance: String): Unit = {
    Option(clusters.get(cluster)).foreach(_.delInstance(instance))
  }

  override def uptInstance(cluster: String, instance: String, status: MySQLStatus): Unit = {
    Option(clusters.get(cluster)).foreach(_.uptInstance(instance, status))
  }

  override def addInstance(cluster: String, instance: String, status: MySQLStatus): Unit = {
    var sentinelCluster = clusters.get(cluster)
    if ( sentinelCluster == null ) {
      sentinelCluster = new SentinelAutoSwitch(cluster)
      clusters.put(cluster, sentinelCluster)
    }
    sentinelCluster.addInstance(instance, status)
  }

  override def uptMasterModel(cluster: String, clusterModel: MasterModel): Unit = {
    var sentinelCluster = clusters.get(cluster)
    if ( sentinelCluster == null ) {
      sentinelCluster = new SentinelAutoSwitch(cluster)
      clusters.put(cluster, sentinelCluster)
    }
    sentinelCluster.uptMasterModel(clusterModel)
  }
}
