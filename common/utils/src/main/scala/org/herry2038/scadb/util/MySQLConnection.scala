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
package org.herry2038.scadb.util


import java.sql._
import java.util
import scala.collection.mutable
import scala.collection.JavaConversions._

import scala.util.Try

class MySQLConnection(val url: String, val user: String, val pass: String) extends Logging {
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
      con = DriverManager.getConnection(url, user, pass)
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


  def executeUpdate(sql: String): Int = {
    var stmt: Statement = null
    if ( !isConnected )
      throw new SQLException("connection error!")
    try {
      stmt = con.createStatement
      stmt.executeUpdate(sql)
    } catch {
      case ex: SQLException =>
        throw ex ;
    } finally {
      closeClosable(stmt)
    }
  }


  def executeQuery(sql: String) : MySQLResult = {
    if ( !isConnected )
      throw new SQLException("connection error!")
    var stmt: Statement = null
    var rs : ResultSet = null
    try {
      stmt = con.createStatement()
      rs = stmt.executeQuery(sql) ;
      val results = new util.ArrayList[util.List[String]]() ;
      val columnCnt = rs.getMetaData().getColumnCount() ;
      val meta = new util.ArrayList[String]
      (0 until columnCnt ).foreach{ i =>
        meta.add(rs.getMetaData.getColumnName(i))
      }

      while ( rs.next() ) {
        val result = new util.ArrayList[String]() ;
        for ( i <- 1 to columnCnt )
          result.add(rs.getString(i))
        results.add(result)
      }
      new MySQLResult(meta, results)
    } catch {
      case ex: SQLException =>
        throw ex
    } finally {
      closeClosable(rs)
      closeClosable(stmt)
    }
  }


}

object MySQLConnection {
  class MySQLResult(val meta: java.util.List[String], val result: util.List[util.List[String]]) {
    private val metaReverse: Map[String, Int] = (meta zip 0.until(meta.size)).foldLeft(Map[String, Int]()) ((m, v) => m + (v._1 -> v._2))
    def row(i: Int): util.List[String] = result.get(i)
    def col(row: Int, col: Int) = result.get(row).get(col)
    def col(row: Int, colName: String) = result.get(row).get(metaReverse(colName))
  }

  def closeClosable(o: AutoCloseable) = Try(Option(o).foreach(_.close))
}