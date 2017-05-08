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
package org.herry2038.scadb.conf.business

import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.{NodeCacheListener, PathChildrenCacheEvent, PathChildrenCacheListener}
import org.apache.curator.utils.ZKPaths
import org.herry2038.scadb.util.Log

/**
 * Created by Administrator on 2016/2/22.
 */

object ScadbConfBusinessListener {

  class ScadbConfBusinessDataListener(val busi: ScadbConfBusiness) extends NodeCacheListener {
    val log = Log.get[ScadbConfBusinessDataListener]
    override def nodeChanged(): Unit = {
      log.info("business data listener changed!")
      busi.uptBusiness()
    }
  }

  class ScadbConfBusinessListener(val busi: ScadbConfBusiness) extends PathChildrenCacheListener {
    val log = Log.get[ScadbConfBusinessListener]


    override def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent): Unit = {
      event.getType match {
        case PathChildrenCacheEvent.Type.CHILD_ADDED =>
          val subBusiness = ZKPaths.getNodeFromPath(event.getData.getPath)
          subBusiness match {
            case "tables" =>
              busi.addTablesListener
            case "rules" =>
              busi.addRulesListener
            case "auths" =>
              busi.addUserListener
            case "whitelist" =>
              busi.addWhiteListListener
            case opt: String =>
              log.info("unknown option of business {}", opt)
          }
        case PathChildrenCacheEvent.Type.CHILD_UPDATED =>
          val subBusiness = ZKPaths.getNodeFromPath(event.getData.getPath)
          log.info("ignore business 's sub project update {}", subBusiness)
        case PathChildrenCacheEvent.Type.CHILD_REMOVED =>
          val subBusiness = ZKPaths.getNodeFromPath(event.getData.getPath)
          subBusiness match {
            case "tables" =>
              busi.delTablesListener
            case "rules" =>
              busi.delRulesListener
            case "auths" =>
              busi.delUserListener
            case "whitelist" =>
              busi.addWhiteListListener
            case opt: String =>
              log.info("unknown option of business {}", opt)
          }
        case a: Any =>
          log.info("unhandled path node msg:{}", a)
      }
    }
  }


  class ScadbConfBusinessTablesListener (val busi: ScadbConfBusiness) extends PathChildrenCacheListener {
    val log = Log.get[ScadbConfBusinessTablesListener]

    override def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent): Unit = {
      event.getType match {
        case PathChildrenCacheEvent.Type.CHILD_ADDED =>
          val table = ZKPaths.getNodeFromPath(event.getData.getPath)

          busi.addTable(table, new String(event.getData.getData))
        case PathChildrenCacheEvent.Type.CHILD_UPDATED =>
          val table = ZKPaths.getNodeFromPath(event.getData.getPath)
          busi.uptTable(table, new String(event.getData.getData))

        case PathChildrenCacheEvent.Type.CHILD_REMOVED =>
          val table = ZKPaths.getNodeFromPath(event.getData.getPath)
          busi.delTable(table)
        case a: Any =>
          log.info("unhandled path node msg:{}", a)
      }
    }
  }


  class ScadbConfBusinessRulesListener (val busi: ScadbConfBusiness) extends PathChildrenCacheListener {
    val log = Log.get[ScadbConfBusinessRulesListener]

    override def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent): Unit = {
      event.getType match {
        case PathChildrenCacheEvent.Type.CHILD_ADDED =>
          val rule = ZKPaths.getNodeFromPath(event.getData.getPath)
          busi.addRule(rule, new String(event.getData.getData))
        case PathChildrenCacheEvent.Type.CHILD_UPDATED =>
          val rule = ZKPaths.getNodeFromPath(event.getData.getPath)
          busi.uptRule(rule, new String(event.getData.getData))
        case PathChildrenCacheEvent.Type.CHILD_REMOVED =>
          val rule = ZKPaths.getNodeFromPath(event.getData.getPath)
          busi.delRule(rule)
        case a: Any =>
          log.info("unhandled path node msg:{}", a)
      }
    }
  }

  class ScadbConfBusinessAuthsListener (val busi: ScadbConfBusiness) extends PathChildrenCacheListener {
    val log = Log.get[ScadbConfBusinessRulesListener]

    override def childEvent(client: CuratorFramework, event: PathChildrenCacheEvent): Unit = {
      event.getType match {
        case PathChildrenCacheEvent.Type.CHILD_ADDED =>
          val user = ZKPaths.getNodeFromPath(event.getData.getPath)
          busi.addUser(user, new String(event.getData.getData))
        case PathChildrenCacheEvent.Type.CHILD_UPDATED =>
          val user = ZKPaths.getNodeFromPath(event.getData.getPath)
          busi.uptUser(user, new String(event.getData.getData))
        case PathChildrenCacheEvent.Type.CHILD_REMOVED =>
          val user = ZKPaths.getNodeFromPath(event.getData.getPath)
          busi.delUser(user)
        case a: Any =>
          log.info("unhandled path node msg:{}", a)
      }
    }
  }

  class ScadbConfBusinessWhitelistListener(val busi: ScadbConfBusiness) extends NodeCacheListener {
    val log = Log.get[ScadbConfBusinessWhitelistListener]
    override def nodeChanged(): Unit = {
      log.info("whitelist listener changed!")
      busi.uptWhitelist()
    }
  }
}
