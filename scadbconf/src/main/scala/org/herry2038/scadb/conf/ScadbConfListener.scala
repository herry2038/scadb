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
package org.herry2038.scadb.conf

import java.util

import ScadbConf.{User, Business, Rule, Table}

/**
 * Created by Administrator on 2016/2/24.
 */
trait ScadbConfListener {
  def uptWhitelist(business: String, whiteLists: util.List[String]): Unit

  def addUser(business: String, user: String, u: User): Unit
  def uptUser(business: String, user: String, u: User): Unit
  def delUser(business: String, user: String): Unit

  def uptBusiness(business: String, busi: Business) :Unit
  def addTable(business: String, table: String, t: Table): Unit
  def uptTable(business: String, table: String, t: Table): Unit
  def delTable(business: String, table: String): Unit
  def addRule(business: String, rule: String, r: Rule): Unit
  def uptRule(business: String, rule: String, r: Rule): Unit
  def delRule(business: String, rule: String): Unit
}
