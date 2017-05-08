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

import java.util.ArrayList

class ScadbCallbackSub(val statement: ScadbStatementMulti) extends ScadbCallback {
    var fields:ArrayList[ScadbField] = new ArrayList[ScadbField]
    
    
	override def onError(errno:Int,errmsg: String):Unit = {
	    errorNo = errno
	    errorMsg = errmsg
	    
	}
	
	override def onField(table:String,field:String,len: Long,fType:Byte,flags:Int) = {
	    val f = new ScadbField(table,field,len,fType,flags)
	    fields.add(f)	    
	}
	
	override def onRow(row: java.util.List[Object]) = {
	    
	}
	
	override def onResultEnd() = {
	    
	}
	
	override def onUpdate(updateCount:Long,lastId:Long,status:Int,warningCount:Int,msg:String) = {
	    this.updateCount = updateCount 
	    this.lastId = lastId
	}
	
}