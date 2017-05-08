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
package org.herry2038.scadb.admin.leader

import java.util.concurrent.CountDownLatch
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener
import org.apache.curator.framework.state.ConnectionState
import org.herry2038.scadb.admin.server.AdminConf
import org.herry2038.scadb.util.Log

/**
 * Created by Administrator on 2016/9/2.
 */
class AdminLeaderListener extends LeaderSelectorListener {
  private val log = Log.get[AdminLeaderListener]
  private var leaderLatch: CountDownLatch = null


  override def takeLeadership(client: CuratorFramework): Unit ={
    log.info(s"take leader ship ${AdminConf.node}")

    this.synchronized {
      leaderLatch = new CountDownLatch(1)
      try {
        AdminMaster.start
      } catch {
        case ex: Throwable =>
          log.error("leader get a error:", ex)
          leaderLatch.countDown()
      }
    }
    leaderLatch.await()

    this.synchronized {
      AdminMaster.stop
      leaderLatch = null
    }

    log.info(s"release leader ship ${AdminConf.node}")
  }

  override def stateChanged(client: CuratorFramework, newState: ConnectionState): Unit = {
    if ( (newState == ConnectionState.SUSPENDED) || (newState == ConnectionState.LOST) )  {
      this.synchronized {
        if ( leaderLatch != null ) {
          leaderLatch.countDown();
        }
      }
    }
  }
}
