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

import java.util
import java.util.concurrent.ConcurrentHashMap
import com.google.gson.Gson
import com.google.gson.reflect.TypeToken
import org.apache.curator.framework.recipes.cache.{NodeCache, PathChildrenCache}
import ScadbConfBusinessListener._
import org.herry2038.scadb.conf.{ScadbConf, ScadbConfListener}
import ScadbConf.{User, Business, Rule, Table}
import org.herry2038.scadb.conf.{ScadbConfListener, ScadbConf}
import org.herry2038.scadb.conf.ScadbConf.{Business, User, Rule, Table}
import org.herry2038.scadb.util.Log

import scala.collection.mutable

/**
 * Created by Administrator on 2016/2/22.
 */
class ScadbConfBusiness(val business: String) {
  val log = Log.get[ScadbConfBusiness]
  var defaultMySQL: String = null
  var mysqls: util.List[String] = new util.ArrayList[String]

  val tables = new ConcurrentHashMap[String, Table]()
  val rules = new ConcurrentHashMap[String, Rule]()
  val users = new ConcurrentHashMap[String, User]()

  var businessPathCache: PathChildrenCache = _
  val businessDataCache = ScadbConf.dataCache(ScadbConf.businessesPath + "/" + business, new ScadbConfBusinessDataListener(this))

  val listeners = new mutable.ArrayBuffer[ScadbConfListener]()

  var tablePathCache: PathChildrenCache = _
  var rulePathCache: PathChildrenCache = _
  var userPathCache: PathChildrenCache = _
  var whitelistDataCache: NodeCache = null

  def start(): Unit= {
    ScadbConf.wrapperWithLock{ ()=>
      if ( businessDataCache.getCurrentData == null ) {
        System.out.println("data is null!")
      } else {
        uptBusiness()
      }
    }
  }

  def stop() = {
    ScadbConf. wrapperWithLock{ () =>
      delTablesListener
      delRulesListener
      delUserListener
      businessDataCache.close()
      businessPathCache.close()
      businessPathCache = null
    }
  }

  def registerListener(listener: ScadbConfListener): Unit = {
    ScadbConf. wrapperWithLock { () =>
      listeners += listener
    }
  }


  def addTablesListener: Unit = {
    ScadbConf. wrapperWithLock{ () =>
      log.info("add table listener for {}", business)
      tablePathCache = ScadbConf.pathCache(ScadbConf.businessesPath + "/" + business + "/tables", new ScadbConfBusinessTablesListener(this))
    }
  }

  def delTablesListener: Unit = {
    ScadbConf. wrapperWithLock{ () =>
      tablePathCache.close()
      tablePathCache = null
    }
  }

  def addRulesListener: Unit = {
    ScadbConf. wrapperWithLock{ () =>
      log.info("add rules listener for {}", business)
      rulePathCache = ScadbConf.pathCache(ScadbConf.businessesPath + "/" + business + "/rules", new ScadbConfBusinessRulesListener(this))
    }
  }

  def delRulesListener: Unit = {
    ScadbConf. wrapperWithLock{ () =>
      rulePathCache.close()
      rulePathCache = null
    }
  }

  def addUserListener: Unit = {
    ScadbConf. wrapperWithLock{ () =>
      log.info("add users listener for {}", business)
      userPathCache = ScadbConf.pathCache(ScadbConf.businessesPath + "/" + business + "/auths", new ScadbConfBusinessAuthsListener(this))
    }
  }

  def delUserListener: Unit = {
    ScadbConf. wrapperWithLock{ () =>
      userPathCache.close()
      userPathCache = null
    }
  }

  def addWhiteListListener: Unit = {
    ScadbConf. wrapperWithLock{ () =>
      log.info("add whitelist listener for {}", business)
      whitelistDataCache = ScadbConf.dataCache(ScadbConf.businessesPath + "/" + business + "/whitelist", new ScadbConfBusinessWhitelistListener(this))
    }
  }

  def delWhiteListListener: Unit = {
    ScadbConf. wrapperWithLock{ () =>
      log.info("del whitelist listener for {}", business)
      whitelistDataCache.close
      whitelistDataCache = null
    }
  }

  def delRule(rule: String): Unit = {
    ScadbConf. wrapperWithLock{ () =>
      log.info("delete rule {}", rule)
      rules.remove(rule)
      listeners.map(_.delRule(business, rule))
    }
  }

  def uptRule(rule: String, s: String): Unit = {
    ScadbConf. wrapperWithLock{ () =>
      val r = new Gson().fromJson(s, classOf[Rule])
      log.info(String.format("update rule: %s, rule info: %s", rule, s))
      rules.put(rule, r)
      listeners.map(_.uptRule(business, rule, r))
    }
  }

  def addRule(rule: String, s: String): Unit = {
    ScadbConf. wrapperWithLock{ () =>
      val r = new Gson().fromJson(s, classOf[Rule])
      log.info(String.format("add rule: %s, rule info: %s", rule, s))
      if ( rules.putIfAbsent(rule, r) == null )
        listeners.map(_.addRule(business, rule, r))
    }
  }

  def addTable(table: String, tabledef: String) = {
    ScadbConf.wrapperWithLock { () =>
      val t = new Gson().fromJson(tabledef, classOf[Table])
      log.info(String.format("add table: %s, table info: %s", table, t))
      if ( tables.putIfAbsent(table, t) == null )
        listeners.map(_.addTable(business, table, t))
    }
  }


  def delTable(table: String): Unit = {
    ScadbConf. wrapperWithLock{ () =>
      log.info(String.format("drop table: %s", table))
      tables.remove(table)
      listeners.map(_.delTable(business, table))
    }
  }

  def uptTable(table: String, tabledef: String): Unit = {
    ScadbConf. wrapperWithLock{ () =>
      val t = new Gson().fromJson(tabledef, classOf[Table])
      log.info(String.format("update table: %s, table info: %s", table, t))
      tables.put(table, t)
      listeners.map(_.uptTable(business, table, t))
    }
  }

  def addUser(user: String, userData: String): Unit = {
    ScadbConf. wrapperWithLock{ () =>
      val t = new Gson().fromJson(userData, classOf[User])
      log.info(s"add user: ${user}, userInfo : ${t}")
      if ( users.putIfAbsent(user, t) == null )
        listeners.map(_.addUser(business, user, t))
    }
  }

  def uptUser(user: String, userData: String): Unit = {
    ScadbConf. wrapperWithLock{ () =>
      val t = new Gson().fromJson(userData, classOf[User])
      log.info(s" user: ${user}, userInfo : ${t}")
      users.put(user, t)
      listeners.map(_.uptUser(business, user, t))
    }
  }

  def delUser(user: String): Unit = {
    ScadbConf. wrapperWithLock{ () =>
      log.info(s"del user: ${user}")
      users.remove(user)
      listeners.map(_.delUser(business, user))
    }
  }

  def uptBusiness(): Unit = {
    ScadbConf.wrapperWithLock{ () =>
      val businessData = new String(businessDataCache.getCurrentData.getData)
      val busiTmp = new Gson().fromJson(businessData, classOf[Business])
      val busi = new Business(
        busiTmp.default, busiTmp.mysqls, busiTmp.dbNums, busiTmp.dbPrefix,
        if ( busiTmp.minConnections <= 0 ) 20 else busiTmp.maxConnections,
        if ( busiTmp.maxConnections <= 0 ) 100 else busiTmp.maxConnections,
        Option(busiTmp.charset).getOrElse("utf8")
        )

      log.info(String.format("business:%s,updated:%s",business, businessData))
      defaultMySQL = busi.default ;
      mysqls = busi.mysqls ;

      if ( businessPathCache == null ) {
        businessPathCache = ScadbConf.pathCache(ScadbConf.businessesPath + "/" + business, new ScadbConfBusinessListener(this))
      }

      listeners.map(_.uptBusiness(business, busi))
    }
  }

  def uptWhitelist(): Unit = {
    ScadbConf.wrapperWithLock{ () =>
      val whitelist = new String(whitelistDataCache.getCurrentData.getData)
      val whiteLists = new Gson().fromJson(whitelist,
        new TypeToken[java.util.List[String]]() {
        }.getType()).asInstanceOf[java.util.List[String]]
      listeners.map(_.uptWhitelist(business, whiteLists))
    }
  }
}


