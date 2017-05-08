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
package org.herry2038.scadb.admin.model

import org.herry2038.scadb.conf.ScadbConf.{Table, Rule, Business}

/**
 * Created by Administrator on 2016/3/9.
 */
class BusinessWrapper(val name: String, busi: Business) {
  def this(name: String) = this(name, null)
  var busiData: Business = busi
  val rules = new java.util.concurrent.ConcurrentHashMap[String, Rule]()
  val tables = new java.util.concurrent.ConcurrentHashMap[String, Table]()

  def uptBusiData(busiData: Business) = this.busiData = busiData

  def addTable(tableName: String, tableDef: Table) = tables.putIfAbsent(tableName, tableDef)

  def delTable(tableName: String) = tables.remove(tableName)

  def uptTable(tableName: String, tableDef: Table) = tables.replace(tableName, tableDef)

  def addRule(ruleName: String, rule: Rule) = rules.putIfAbsent(ruleName, rule)

  def delRule(ruleName: String) = rules.remove(ruleName)

  def uptRule(ruleName: String, rule: Rule) = rules.replace(ruleName, rule)

  // 功能函数
  def getRule(ruleName: String) = rules.get(ruleName)

}
