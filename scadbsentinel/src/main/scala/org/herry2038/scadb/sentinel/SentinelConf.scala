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

import org.herry2038.scadb.clusters.Clusters
import org.herry2038.scadb.conf.ScadbConf
import org.herry2038.scadb.util.{Logging, Config}

object SentinelConf extends Logging {
  var zookeeper: String = "localhost:2181"
  var parallelDegree = 16

  var detectorInterval = 5000
  var maxFailedDetectorInterval = 60000
  var statisticsInterval = 10000
  var slaveDelaySecondsChangeInterval = 20
  var zoneId = "*"

  var clusters: Clusters = null

  def load(): Clusters = {
    Config.loadConfig("/scadb.properties")
    zookeeper = Config.instance.getString("zookeeper", zookeeper)
    parallelDegree = Config.instance.getInt("parallelDegree", parallelDegree)
    detectorInterval = Config.instance.getInt("detectorInterval", detectorInterval)
    maxFailedDetectorInterval = Config.instance.getInt("maxFailedDetectorInterval", maxFailedDetectorInterval)
    statisticsInterval = Config.instance.getInt("statisticsInterval", statisticsInterval)
    zoneId = Config.instance.getString("zone_id", zoneId)

    slaveDelaySecondsChangeInterval = Config.instance.getInt("slaveDelaySecondsChangeInterval", slaveDelaySecondsChangeInterval)

    val switcher = Config.instance.getString("switcher", "org.herry2038.scadb.switcher.MysqlSwitcherCommon")

    try {
      val switcherObject = Class.forName(switcher).newInstance().asInstanceOf[org.herry2038.scadb.switcher.MysqlSwitcher]
      SentinelAutoswitchService.switcher = switcherObject

    } catch {
      case ex: Throwable =>
        error(s"load switcher [${switcher}] failed!", ex)
        System.exit(-1)
    }
    ScadbConf.start(zookeeper)
    SentinelAutoswitchService.startLeaderSelector()
    clusters = new Clusters
    clusters
  }
}
