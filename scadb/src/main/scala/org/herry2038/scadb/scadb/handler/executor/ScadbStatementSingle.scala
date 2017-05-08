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
package org.herry2038.scadb.scadb.handler.executor

import org.herry2038.scadb.mysql.codec.MessageTrack
import org.herry2038.scadb.mysql.server.MySQLServerConnectionHandler
import org.herry2038.scadb.scadb.conf.ScadbMySQLPools
import org.herry2038.scadb.util.Log
import scala.util.{Try,Success,Failure}
import org.herry2038.scadb.db.QueryResult
import org.herry2038.scadb.db.util.ExecutorServiceUtils.CachedExecutionContext

object ScadbStatementSingle {
  val log = Log.get[ScadbStatementSingle]
}

class ScadbStatementSingle(val handler: MySQLServerConnectionHandler,val sql:String,val server:String,val startTime: Long, val requestType: Int, val isRead: Boolean = false, val mt: MessageTrack = null) extends ScadbStatement(handler, startTime, requestType) {
	import ScadbStatementSingle.log


	def execute(): Int = {	   
	    val pool = ScadbMySQLPools.getPool(server,isRead).getOrElse{
				log.error("cannot find server:{}",server)
				error(-1,"cannot get inner pool of [" + server + "]")
				return 0
	    }

	    if ( pool == null ) {
				log.error("cannot find server:{} ==== is null",server)
				error(-1,"get inner pool of [" + server + "] is null")
				return 0
			}
	    pool.sendQuery(sql, mt).onComplete{
	        case Success(v) =>
						/*
						if ( func.isDefined ) {
							write(func.get(v))
						} else
	          	write(v)
	          */
						write(v)
	        case Failure(e) =>
	            log.error("send query {} error: {} {} !", sql, e.getMessage,"")
	            error(-1,e.getMessage)
	    }
    	0
	}

    
}

