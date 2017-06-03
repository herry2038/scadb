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

package org.herry2038.scadb.clusters

import org.apache.curator.framework.recipes.cache.PathChildrenCache
import org.herry2038.scadb.conf.ScadbConf
import org.herry2038.scadb.conf.cluster.ClusterConf
import org.herry2038.scadb.conf.common.{ScadbConfPathListenerWithSubPath, SubObject, Creator, PathObject}
import org.herry2038.scadb.sentinel.{SentinelAutoswitchService, SentinelDetectorService}


class BusinessClusters(val path: String, val key: String) extends Creator[ClusterConf] with PathObject[ClusterConf] with SubObject {
  val businessCache: PathChildrenCache = ScadbConf.pathCache(path, new ScadbConfPathListenerWithSubPath[ClusterConf, BusinessClusters](this))

  def close: Unit = businessCache.close

  override def create(path: String, key: String): ClusterConf = {
    val cc = super.create(path, key)
    cc.registerListener(SentinelDetectorService)
    cc.registerListener(SentinelAutoswitchService)
    cc
  }
}
