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
package org.herry2038.scadb.conf.utils

import org.apache.zookeeper.KeeperException.NoNodeException
import org.herry2038.scadb.conf.ScadbConf

/**
 * Created by Administrator on 2016/2/23.
 */
object ZKUtils {

  def ensureDelete(path: String) = {
    try {
      ScadbConf.client.delete().deletingChildrenIfNeeded().forPath(path)
    } catch {
      case e : NoNodeException =>
    }
  }

  def create(path: String ,data: String = null) = {
    if ( data == null )
      ScadbConf.client.create().forPath(path)
    else
      ScadbConf.client.create().forPath(path, data.getBytes)
  }

  def createBusiness() = {
    val client = ScadbConf.client

    ensureDelete(s"${ScadbConf.basePath}")
    client.create().forPath(s"${ScadbConf.basePath}")
    client.create().forPath(s"${ScadbConf.basePath}/businesses")
    client.create().forPath(s"${ScadbConf.basePath}/businesses/test", "{'default':'set1.test_service', 'mysqls':['set1.test_service']}".getBytes)
    client.create().forPath(s"${ScadbConf.basePath}/businesses/test/tables")
    client.create().forPath(s"${ScadbConf.basePath}/businesses/test/rules")
    client.create().forPath(s"${ScadbConf.basePath}/businesses/test/tables/a", "{'column':'id','ruleName':'hash_int'}".getBytes)
    client.create().forPath(s"${ScadbConf.basePath}/businesses/test/rules/hash_int", "{'algorithm':'HashIntAlgorithm','partitions':'2'}".getBytes)
  }

  def createTable(t: String, tabledef: String) = {
    val client = ScadbConf.client
    client.create().forPath(s"${ScadbConf.basePath}/businesses/test/tables/" + t, tabledef.getBytes)
  }

  def createRule(r: String, ruledef: String) = {
    val client = ScadbConf.client
    client.create().forPath(s"${ScadbConf.basePath}/businesses/test/rules/" + r, ruledef.getBytes)
  }

  def createJob(job: String, jobInfo: String) = {
    val client = ScadbConf.client
    client.create().forPath(s"${ScadbConf.basePath}/jobs/" + job, jobInfo.getBytes)
  }

}

