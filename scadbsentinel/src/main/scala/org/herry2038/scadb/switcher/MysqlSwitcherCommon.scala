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
package org.herry2038.scadb.switcher

import com.google.gson.Gson
import org.herry2038.scadb.conf.ScadbConf
import org.herry2038.scadb.conf.cluster.{ClusterConf}
import org.herry2038.scadb.conf.cluster.ClusterModel.{MasterModel, MySQLStatus}
import org.herry2038.scadb.util.Logging



class MysqlSwitcherCommon extends MysqlSwitcher with Logging {
  override def isBetterThan(host: String, status: MySQLStatus, compareToHost: String, compareTo: MySQLStatus): Boolean = {
    if ( status.delaySeconds != -1 )
      if (compareTo.delaySeconds != -1 ) status.delaySeconds < compareTo.delaySeconds else true
    else
      false
  }

  override def switch(cluster: SentinelAutoSwitch, destination: String): (Boolean, MasterModel) = {
    val parts = cluster.cluster.split("\\.")
    val path = ClusterConf.path(parts(0), parts(1))
    val model = new MasterModel(destination, cluster.clusterModel.switchMode)

    try {
      ScadbConf.client.setData().forPath(path, new Gson().toJson(model).getBytes)
      (true, model)
    } catch {
      case ex: Exception =>
        error(s"sync to zk url: ${path} error!", ex)
        (false, null)
    }
  }
}
