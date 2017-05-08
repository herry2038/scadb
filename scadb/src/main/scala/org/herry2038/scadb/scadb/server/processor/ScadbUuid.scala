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
package org.herry2038.scadb.scadb.server.processor

import java.security.SecureRandom
import java.util.concurrent.atomic.AtomicInteger

import org.apache.zookeeper.CreateMode
import org.herry2038.scadb.conf.ScadbConf
import org.herry2038.scadb.scadb.conf.ScadbConfig
import org.herry2038.scadb.util.Logging
import scala.collection.JavaConversions._

import scala.util.Try

/**
 * Created by Administrator on 2017/3/30.
 */
object ScadbUuid extends Logging {
  private val random = new SecureRandom()
  val NEXT_COUNTER = new AtomicInteger(random.nextInt())
  private val COUNTER_MASK = 0x000fffff
  var RUNNER_ID = -1
  private val RUNNER_MASK = 0x1ff



  def uuid = {
    val ts = (System.currentTimeMillis() / 1000 )
    val counter = NEXT_COUNTER.getAndIncrement & COUNTER_MASK
    ts << 31 | RUNNER_ID << 20 | counter
  }

  def getRunnerId = {
    val path = ScadbConf.businessesPath + "/" + ScadbConfig.conf.business + "/runners"

    if ( ScadbConf.client.checkExists().forPath(path) == null ) {
      Try(ScadbConf.client.create().forPath(path))
    }
    var tryTimes = 1
    while ( RUNNER_ID == -1) {
      info(s"try get runner id ${tryTimes} ...")
      val currentRunners = ScadbConf.client.getChildren.forPath(path).map(_.toInt)
      var pid = random.nextInt() & RUNNER_MASK
      while (currentRunners.contains(pid)) {
        pid += 1
        if (pid > RUNNER_MASK)
          pid = 0
      }
      Try(ScadbConf.client.create().withMode(CreateMode.EPHEMERAL).forPath(path + "/" + pid)).map(_ => RUNNER_ID = pid)
    }
  }
}
