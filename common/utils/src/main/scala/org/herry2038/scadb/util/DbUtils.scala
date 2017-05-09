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

import org.apache.commons.dbcp.BasicDataSource

import scala.util.Try

object DbUtils {
  def createPool(mysql: String, user: String, pass: String, db: Option[String] = None, poolSize: Int = 20): BasicDataSource = {
    val bds = new BasicDataSource
    bds.setUrl("jdbc:mysql://" + mysql + "/" + db.getOrElse("") + "?allowMultiQueries=true&jdbcCompliantTruncation=false&useUnicode=true&characterEncoding=UTF-8");
    bds.setDriverClassName("com.mysql.jdbc.Driver");
    bds.setUsername(user)
    bds.setPassword(pass)
    bds.setInitialSize(0)
    val realPoolSize = if ( poolSize > 20 )  poolSize else 20
    bds.setMaxActive(realPoolSize)
    bds.setMinIdle(0)
    bds.setMaxIdle(20)
    bds.setMaxWait(600)
    bds
  }

  def closePool(bds: BasicDataSource) = Try(Option(bds).foreach(_.close))


  def executeUpdateSingleStatement(bds: BasicDataSource, sql: String): Int = {
    var conn: Connection = null
    var stmt: Statement = null
    try {
      conn = bds.getConnection
      stmt = conn.createStatement
      stmt.executeUpdate(sql)
    } catch {
      case ex: SQLException =>
        throw ex ;
    } finally {
      closeClosable(stmt)
      closeClosable(conn)
    }
  }

  def executeQuery(bds: BasicDataSource, sql: String,objs: String*) : java.util.List[java.util.List[String]] = {
    var conn: Connection = null
    var stmt: PreparedStatement = null
    var rs : ResultSet = null
    try {
      conn = bds.getConnection
      stmt = conn.prepareStatement(sql) ;
      for ( i <- 1 to objs.size ) {
        stmt.setString(i, objs(i-1))
      }

      rs = stmt.executeQuery() ;
      val results = new util.ArrayList[util.List[String]]() ;
      val columnCnt = rs.getMetaData().getColumnCount() ;
      while ( rs.next() ) {
        val result = new util.ArrayList[String]() ;
        for ( i <- 1 to columnCnt )
          result.add(rs.getString(i))
        results.add(result)
      }

      results ;
    } catch {
      case ex: SQLException =>
        throw new RuntimeException(s"execute sql ${sql}, error:" +  ex.getMessage()) ;
    } finally {
      closeClosable(rs)
      closeClosable(stmt)
      closeClosable(conn)
    }
  }


  def closeClosable(c: AutoCloseable): Unit = Try(Option(c).foreach(_.close))
}
