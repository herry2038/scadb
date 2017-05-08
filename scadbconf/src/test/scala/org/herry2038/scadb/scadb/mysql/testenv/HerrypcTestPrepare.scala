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
package org.herry2038.scadb.scadb.mysql.testenv

import org.herry2038.scadb.conf.ScadbConf
import org.herry2038.scadb.conf.utils.ZKUtils

/**
 * Created by Administrator on 2016/3/23.
 */
object HerrypcTestPrepare {
  def main(args: Array[String]) {
    ScadbConf.start("herrypc:2181")

    // real password is .LqZZOO17.A

    ZKUtils.ensureDelete(s"${ScadbConf.basePath}")
    ZKUtils.create(s"${ScadbConf.basePath}")
    ZKUtils.create(s"${ScadbConf.basePath}/admins")
    ZKUtils.create(s"${ScadbConf.basePath}/admins/nodes")
    ZKUtils.create(s"${ScadbConf.basePath}/admins/leader")
    ZKUtils.create(s"${ScadbConf.basePath}/businesses")
    ZKUtils.create(s"${ScadbConf.basePath}/jobs")
    ZKUtils.create(s"${ScadbConf.basePath}/jobs/test")
    ZKUtils.create(s"${ScadbConf.basePath}/businesses/test","{'default':'set1.test_service', 'mysqls':['set1.test_service'], 'dbNums': 2, 'dbPrefix': 'db'}")
    ZKUtils.create(s"${ScadbConf.basePath}/businesses/test/rules")
    ZKUtils.create(s"${ScadbConf.basePath}/businesses/test/rules/hash_int_2", "{'algorithm':'HashIntAlgorithm','partitions':'2'}")
    ZKUtils.create(s"${ScadbConf.basePath}/businesses/test/tables")
    ZKUtils.create(s"${ScadbConf.basePath}/businesses/test/tables/a","{'column':'id','ruleName':'hash_int_2','mysqls':['set1.test_service']}")
    ZKUtils.create(s"${ScadbConf.basePath}/businesses/test/whitelist","['127.0.0.1','172.26.82.23','172.26.82.24']")
    ZKUtils.create(s"${ScadbConf.basePath}/businesses/test/auths")
    ZKUtils.create(s"${ScadbConf.basePath}/businesses/test/auths/myshard","{'user':'myshard','passwd':'tttt','privilege':1}")


    ZKUtils.create("/cdb/dbsets/set1/test_service", "{'backEndIdleTimeOut':1800000,'balanceType':0,'frontEndIdleTimeOut':1800000,'groups':['group1'],'heatBeatSQL':'select 1','maxCon':1000,'minCon':5,'pass':'sVDnzW7dh87FHSePISdJsA==','slaveThreshold':10,'slowlogTime':1000,'sqlTimeout':300000,'user':'myshard'}")
    ZKUtils.create("/cdb/dbsets/set1/test_service/auth")
    ZKUtils.create("/cdb/dbsets/set1/test_service/auth/myshard", "{'name':'myshard','password':'OffYMkuWrFkfwyXBeKrc2A==','privilege':2,'schemas':['testdb2','testdb']}")
    ZKUtils.create("/cdb/dbsets/set1/test_service/whitelist", "['172.26.82.23','172.26.82.24']")
    ZKUtils.create("/cdb/dbsets/set1/test_service/masters", "herrypc:3306")
    ZKUtils.create("/cdb/dbsets/set1/test_service/masters/herrypc:3308" , "{'delaySeconds':0,'down':false,'group':'group1','status':'START','uuid':'test_uuid'}")
    ZKUtils.create("/cdb/dbsets/set1/test_service/masters/herrypc:3306" , "{'delaySeconds':0,'down':false,'group':'group1','status':'START','uuid':'test_uuid'}")
    ZKUtils.create("/cdb/dbsets/set1/test_service/slaves")
    ZKUtils.create("/cdb/dbsets/set1/test_service/slaves/herrypc:3307", "{'delaySeconds':0,'down':false,'group':'group2','status':'START','uuid':'test_uuid'}")
  }

}
