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

import org.herry2038.scadb.mysql.MySQLQueryResult
import org.herry2038.scadb.mysql.message.server._
import org.herry2038.scadb.db.QueryResult
import org.herry2038.scadb.mysql.server.MySQLServerConnectionHandler
import org.herry2038.scadb.scadb.server.processor.Statistics


abstract class ScadbStatement(handler: MySQLServerConnectionHandler, startTime: Long, requestType: Int) {

    def executeEnclose() : Unit = {
        try {
            execute()
        } catch {
            case e1: ScadbExecuteException =>
                error(e1.errCode,e1.errMsg)            
            case e2: Throwable =>
                error(-1,e2.getMessage)           
        }
    }
    
	def execute() : Int
	
	def error(errNo: Int, errMsg: String) : Unit = {
	    val msg = new ErrorMessage(errNo,errMsg)
		Statistics.request(requestType, true, startTime)
	    handler.write(msg)
	}
	
	def error(cb: ScadbCallback): Unit = {
	    val msg = new ErrorMessage(cb.errorNo, cb.errorMsg)
		Statistics.request(requestType, true, startTime)
	    handler.write(msg)
	}
	
	def write(result : QueryResult) = result.rows match {
	    case None =>
				val msg = new OkMessage(result.rowsAffected,
					result match {
					case mysqlresult: MySQLQueryResult => mysqlresult.lastInsertId
					case _ => 0
					},
					0,0,result.statusMessage)
				Statistics.request(requestType, false, startTime)
	      handler.write(msg)
	    case Some(r) =>
				Statistics.request(requestType, false, startTime)
	        handler.write(r)
	        /*
	        val qrm = new QueryResultMessage(r.columnNames.size)
	        handler.write(qrm)
	        val registry = new DecoderRegistry(CharsetUtil.UTF_8)
        
	        r.columnNames.foreach{c =>
	        	val msg = new ColumnDefinitionMessage(
	        	        "def", // catalog
	        	        "", // schema
	        	        "", // table
	        	        "", // originalTable
	        	        c,
	        	        c,
	        	        83,
	        	        255,
	        	        ScadbField.DP_TYPE_VARCHAR,
	        	        0,
	        	        0,	        	       
	        	      registry.binaryDecoderFor(ColumnTypes.FIELD_TYPE_VARCHAR, 83),
	        	      registry.textDecoderFor(ColumnTypes.FIELD_TYPE_VARCHAR,83) )
	        	
	        	handler.write(msg)
	        }
	        
	        val fieldEndMsg = new ColumnProcessingFinishedMessage(new EOFMessage(0,0))
	        handler.write(fieldEndMsg)
	        
	        
	        r.foreach{row =>
	        	val resultSetRow = new ResultSetRowMessage    
	            row.foreach{f =>
	                val buf = ByteBufferUtils.mysqlBuffer(1024)
	                //buf.writeLenghtEncodedString(f.asInstanceOf[String], CharsetUtil.UTF_8)
	                buf.writeBytes(f.asInstanceOf[String].getBytes(CharsetUtil.UTF_8))
	        		resultSetRow += buf
	            }
	        	handler.write(resultSetRow)
	        }
	        
	        val eofMessage = new EOFMessage(0,0)
	        handler.write(eofMessage)	        
	        */
	}
}


object ScadbStatement {
  var timeout : Int = 5000
    
  def setTimeout(t : Int ) = {
  	timeout = t
	}

	object ScadbStatementNull extends ScadbStatement(null,0, -1) {
		override def execute(): Int = 0
	}

}

