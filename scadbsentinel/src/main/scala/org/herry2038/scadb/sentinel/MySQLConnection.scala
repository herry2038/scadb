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

package org.herry2038.scadb.sentinel

import java.sql.{DriverManager, SQLException, Connection}

import org.herry2038.scadb.conf.ScadbConf
import org.herry2038.scadb.util.Logging

import scala.util.Try

/**
 * Created by Administrator on 2017/5/31.
 */
class MySQLConnection(val url: String) extends Logging {
  import MySQLConnection._
  private var connectStatus: Boolean = false

  private var con: Connection = null


  def getConnection: Connection = this.synchronized {
    if ( isConnected )
      return con
    else {
      if ( connect )
        return con
      return null
    }
  }

  def connect: Boolean = this.synchronized {
    try {
      con = DriverManager.getConnection(url, ScadbConf.backuser, ScadbConf.backpass)
      connectStatus = true
    } catch {
      case ex: SQLException =>
        connectStatus = false
        error(s"connect to url ${url} error", ex)
    }
    connectStatus
  }


  def close = this.synchronized {
    if ( isConnected ) {
      closeClosable(con)
      connectStatus = false
    }
  }

  def isConnected: Boolean = this.synchronized {
    if ( connectStatus ) {
      try {
        val valid = con.isValid(0)
        if ( ! valid ) {
          closeClosable(con)
          connectStatus = false
        }
        return valid
      } catch {
        case e: SQLException =>
          closeClosable(con)
          connectStatus = false
          return false
      }
    }
    false
  }



}

object MySQLConnection {
  def closeClosable(o: AutoCloseable) = Try(Option(o).foreach(_.close))
}