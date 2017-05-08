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
package org.herry2038.scadb.conf.algorithm

import java.util

import org.herry2038.scadb.conf.ScadbConf
import ScadbConf.Rule
import org.herry2038.scadb.conf.ScadbConf.Rule


/**
 * Created by Administrator on 2016/2/22.
 */
trait Algorithm {


  /**
   *
   * @param (分区编号,分区序号)
   *             分区编号用于确定表名，表名的格式为：源表名 + "_" + 分区编号
   *             分区序号用于确定数据库位置，分区的位置为：分区序号 mod 数据库数目
   * @return  @return (partition, table, mysql)
   */
  def partition(value: String): (String, Int)
  def needDefSubRules(): Boolean
  def init(rule: Rule): Boolean
  def partitionNum: Int
  def partitions(table: String, mysqls: util.List[String], dbPrefix: String, dbNums: Int) : List[(String,String,String)]
}

