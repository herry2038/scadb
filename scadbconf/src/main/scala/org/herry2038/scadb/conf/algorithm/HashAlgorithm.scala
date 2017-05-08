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

import scala.collection.mutable


/**
 * Created by Administrator on 2016/2/25.
 */
class HashAlgorithm extends Algorithm {
  var partitionNum: Int = 0
  override def partition(value: String): (String,Int) = {
    val seq = value.hashCode() % partitionNum
    (seq.toString, seq)
  }

  override def needDefSubRules(): Boolean = false

  override def init(rule: Rule): Boolean = {
    partitionNum = rule.partitions
    true
  }

  override def partitions(tableName: String, mysqls: util.List[String], dbPrefix: String, dbNums: Int): List[(String, String, String)] = {
  (0 until partitionNum).foldRight[List[(String, String, String)]](Nil) { (i, list) =>
      val value = if ( dbNums <= 0 )
        (i.toString, tableName+ "_" + i, mysqls.get(i % mysqls.size))
      else {
        val dbseq = i % dbNums
        (i.toString, dbPrefix + dbseq + "." + tableName+ "_" + i, mysqls.get(i % mysqls.size))
      }
      value :: list
    }
  }
}
