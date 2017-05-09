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
package org.herry2038.scadb.tools

import org.apache.commons.cli.{DefaultParser, CommandLine, Options}

import org.herry2038.scadb.conf.ScadbConf
import org.herry2038.scadb.conf.utils.ZKUtils
import org.herry2038.scadb.util.{DbUtils, Config}

object InitEnv {
  object Args {
    var basePath = ScadbConf.basePath
    var zkPath = "localhost:2181"
    var mysql = "127.0.0.1:3306"
    var mysqluser = "scadb"
    var mysqlpass = "scadb"
    var mysqlDbPrefix = "scadb"
    var dbnums = 0
  }

  def setOptions() = {
    val options = new Options()
    options.addOption( null, "zk", true, "zookeeper address, format HOST:PORT, default localhost:2181")
    options.addOption( null, "basepath", true, "zookeeper 's base path to store scadb, default /scadb" )
    options.addOption( null, "mysql", true, "mysql ip and port, default 127.0.0.1:3306")
    options.addOption( null, "mysqluser", true, "mysql login user, default scadb")
    options.addOption( null, "mysqlpass", true, "mysql login pass, default scadb")
    options.addOption( null, "mysqldbprefix", true, "scadb uses this db or dbprefix to store sharding tables, default scadb")
    options.addOption( null, "dbnums", true, "dividing table into how many dbs, default 0, will use dbprefix only")

    options
  }

  def getOptions(line: CommandLine): Unit = {
    Option(line.getOptionValue("zk")).foreach(Args.zkPath = _)
    Option(line.getOptionValue("basepath")).foreach(Args.basePath = _)

    Option(line.getOptionValue("mysql")).foreach(Args.mysql = _)
    Option(line.getOptionValue("mysqluser")).foreach(Args.mysqluser = _)
    Option(line.getOptionValue("mysqlpass")).foreach(Args.mysqlpass = _)
    Option(line.getOptionValue("mysqldbprefix")).foreach(Args.mysqlDbPrefix = _)
    Option(line.getOptionValue("dbnums")).foreach ( a => Args.dbnums = a.toInt )
  }


  def main(args: Array[String]) {
    val lines = new DefaultParser().parse(setOptions, args)
    getOptions(lines)
    Config.instance.putString("scadb_zk_base_path", Args.basePath)
    ScadbConf.start(Args.zkPath)
    ZKUtils.ensureDelete(s"${ScadbConf.basePath}")
    ZKUtils.create(s"${ScadbConf.basePath}")
    ZKUtils.create(s"${ScadbConf.basePath}/admins")
    ZKUtils.create(s"${ScadbConf.basePath}/admins/nodes")
    ZKUtils.create(s"${ScadbConf.basePath}/admins/leader")
    ZKUtils.create(s"${ScadbConf.basePath}/businesses")
    ZKUtils.create(s"${ScadbConf.basePath}/jobs")
    ZKUtils.create(s"${ScadbConf.basePath}/jobs/test")
    ZKUtils.create(s"${ScadbConf.basePath}/businesses/test",s"{'default':'test.0', 'mysqls':['test.0'], 'dbNums': ${Args.dbnums}, 'dbPrefix': '${Args.mysqlDbPrefix}'}")
    ZKUtils.create(s"${ScadbConf.basePath}/businesses/test/rules")
    ZKUtils.create(s"${ScadbConf.basePath}/businesses/test/rules/rule10", "{'algorithm':'HashIntAlgorithm','partitions':'10'}")
    ZKUtils.create(s"${ScadbConf.basePath}/businesses/test/tables")
    //ZKUtils.create(s"${ScadbConf.basePath}/businesses/test/tables/a","{'column':'id','ruleName':'rule10','mysqls':['test.0']}")
    ZKUtils.create(s"${ScadbConf.basePath}/businesses/test/whitelist","['127.0.0.1']")
    ZKUtils.create(s"${ScadbConf.basePath}/businesses/test/auths")
    ZKUtils.create(s"${ScadbConf.basePath}/businesses/test/auths/test","{'user':'test','passwd':'test','privilege':1}")

    ZKUtils.create(s"${ScadbConf.clustersPath}")
    ZKUtils.create(s"${ScadbConf.clustersPath}/test")
    ZKUtils.create(s"${ScadbConf.clustersPath}/test/0", s"{'currentMaster':'${Args.mysql}','switchMode':'AUTO'}")
    ZKUtils.create(s"${ScadbConf.clustersPath}/test/0/${Args.mysql}", "{'delaySeconds':0,'zoneid':'test_zoneid','status':'START',role:'CANDIDATE'}")

    val pool = DbUtils.createPool(Args.mysql, Args.mysqluser, Args.mysqlpass)
    try {
      DbUtils.executeUpdateSingleStatement(pool, s"create database if not exists ${Args.mysqlDbPrefix}")

      if (Args.dbnums > 0) {
        (0 until Args.dbnums).foreach(i =>
          DbUtils.executeUpdateSingleStatement(pool, s"create database if not exists ${Args.mysqlDbPrefix}${i}"))
      }
    } finally {
      DbUtils.closePool(pool)
    }
    System.exit(0)
  }

}
