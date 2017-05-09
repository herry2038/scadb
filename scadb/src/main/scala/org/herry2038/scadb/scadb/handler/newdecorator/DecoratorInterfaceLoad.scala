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
package org.herry2038.scadb.scadb.handler.newdecorator

import java.io.{File}
import java.util
import java.util.concurrent.{Future, Callable, TimeUnit}
import java.util.concurrent.atomic.AtomicLong

import com.alibaba.druid.sql.ast.SQLStatement
import com.alibaba.druid.sql.dialect.mysql.ast.statement.{MySqlHintStatement, MySqlLoadDataInFileStatement}
import org.herry2038.scadb.db.Connection
import org.herry2038.scadb.mysql.message.server.{ErrorMessage, OkMessage}
import org.herry2038.scadb.db.util.{ExecutorServiceUtils}
import org.herry2038.scadb.scadb.conf.{ScadbMySQLPools, ScadbConfig}
import org.apache.commons.io.{FileUtils, FilenameUtils}
import org.apache.commons.io.filefilter.IOFileFilter
import org.apache.commons.lang3.StringUtils
import org.herry2038.scadb.mysql.server.MySQLServerConnectionHandler
import org.herry2038.scadb.scadb.handler.executor.ScadbStatement
import org.herry2038.scadb.scadb.handler.executor.ScadbStatement.ScadbStatementNull
import org.herry2038.scadb.scadb.handler.newdecorator.load.{LoadUtils, LoadOutputUtils}
import scala.collection.JavaConversions._
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}
import org.herry2038.scadb.db.util.ExecutorServiceUtils.CachedExecutionContext

class DecoratorInterfaceLoad(sql: String, statements: java.util.List[SQLStatement], startTime: Long) extends DecoratorInterface(statements.get(statements.size-1), startTime) {
  import DecoratorInterface._

  private val stmt = statement.asInstanceOf[MySqlLoadDataInFileStatement]
  val tableName = stmt.getTableName.getSimpleName

  private val fieldTerminate = Option(stmt.getColumnsTerminatedBy).map{ terminateBy => val str = terminateBy.toString ; str.charAt(1) }.getOrElse('\t')
  private val fieldEnclosedBy = Option(stmt.getColumnsEnclosedBy).map{ encloseBy => val str = encloseBy.toString ; str.charAt(1)}
  private val localDir = { val str = stmt.getFileName.toString ; str.substring(1, str.length -1) }


  override def readable(): Boolean = false
  override def setPartition(partition: String, prefix: String): Unit = ???

  private var step = 0
  private var concurrncyDegree = ScadbConfig.conf.parallelDegree

  override def decorator(handler: MySQLServerConnectionHandler): ScadbStatement = {
    if ( stmt.isLocal ) return null
    if ( ! getTable(tableName) ) {
      return null
    }
    // 1. check 参数
    if ( statements.size > 1 ) {
      val hint = statements.get(0).asInstanceOf[MySqlHintStatement]
      val hintStr = hint.toString
      if ( hintStr.startsWith(hintsPrefix) ) {
        val endPos = hintStr.indexOf("*/");
        val hintSQL = hintStr.substring(hintsPrefix.length(), endPos).trim()
        val hintStrs = hintSQL.split(" ")
        for ( hint <- hintStrs ) {
          hint.split("=") match {
            case Array("step", step) => this.step = step.toInt
            case Array("concurrent", concurrncyDegree) => this.concurrncyDegree = concurrncyDegree.toInt
            case _ =>
          }
        }
      }
    }

    if ( step == 0 ) {
      step1Decorator(handler)
    } else {
      step2Decorator(handler)
      ScadbStatementNull
    }
  }




  private def step1Decorator(handler: MySQLServerConnectionHandler): ScadbStatement = {
    // 1. 查询key的位置信息
    val (showTableName,realTableName,server) = if ( rule == null ) (tableName, tableName, ScadbConfig.conf.busi.default) else {
      val (partitionNo, mysql, dbPrefix) = ScadbConfig.conf.getPartitionAndServer(rule, "0", ScadbConfig.conf.busi.mysqls)
      ((if (dbPrefix == null) "" else dbPrefix + ".") + tableName + "_" + partitionNo, tableName + "_" + partitionNo,  mysql)
    }
    val pool = ScadbMySQLPools.getPool(server,false).getOrElse{
      log.error("cannot find server:{}",server)
      errmsg = "cannot find server:" + server
      return null
    }

    if ( pool == null ) {
      log.error("cannot find server:{} ==== is null",server)
      errmsg = "cannot find server:" + server
      return null
    }

    val descSql = "DESC " + showTableName
    pool.sendQuery(descSql).onComplete{
      case Success(v) =>
        var stop = false
        var i = 0
        if ( v.rows.isDefined ) {
          val rs = v.rows.get
          while ( i < rs.size && !stop ) {
            if (rs(i).apply(0).asInstanceOf[String].equalsIgnoreCase(table.column)) {
              stop = true
            } else {
              i += 1
            }
          }
        }
        // 2. 启动任务
        if ( stop ) {
          scheduleTasks(handler, i)
        } else {
          handler.write(new ErrorMessage(-1, s"cannot find partition key ${table.column} in table ${tableName}"))
        }
      case Failure(e) =>
        log.error("send query {} error: {} {} !", descSql, e.getMessage,"")
        handler.write(new ErrorMessage(errno, descSql + " error:" + e.getMessage))
    }
    return ScadbStatementNull
  }
  private def scheduleTasks(handler: MySQLServerConnectionHandler, keyPos: Int): Unit = {
    val files = FileUtils.listFiles(new File(localDir), new IOFileFilter {
      override def accept(file: File): Boolean = !file.getName.startsWith("loading_")
      override def accept(dir: File, name: String): Boolean = !name.startsWith("loading_")
    },null)

    val lines = new AtomicLong(0)
    var currentPos = 0L
    var endPos = 0L
    var file: File = null

    // 1. 准备任务条件
    val loadUtils = new LoadOutputUtils(rule, localDir, tableName)


    // 2. 准备任务
    val pool = ExecutorServiceUtils.newFixedPool(concurrncyDegree, "loading-step-1")

    val futures = new util.ArrayList[Future[(Long, String)]]

    for ( file <- files ) {
      val inputStream = FileUtils.openInputStream(file)
      while ( file.length > endPos ) {
        if (file.length - currentPos < ONE_TIME_SIZE * 1.5) {
          endPos = file.length
        } else {
          endPos = currentPos + ONE_TIME_SIZE
          inputStream.skip(ONE_TIME_SIZE - 1)
          while (inputStream.read() != '\n')
            endPos += 1
        }
        val call = createFutureTask1(file, currentPos, endPos, lines, keyPos, loadUtils)
        futures.add(pool.submit(call))
      }
      inputStream.close
      currentPos = 0 ; endPos = 0
    }

    // 3. 执行任务
    pool.shutdown
    pool.awaitTermination(Long.MaxValue, TimeUnit.SECONDS)
    loadUtils.close

    futures.find{ f =>
      val (errno, _) = f.get
      errno < 0
    } match {
      case Some(x) =>
        val (errno, errmsg) = x.get
        handler.write(new ErrorMessage(errno.toInt, errmsg))
      case None =>
        handler.write(new OkMessage(lines.get, 0L,0,0, null))
    }
  }

  private def createFutureTask1(file: File, begin: Long, end: Long, lines: AtomicLong, keyPos: Int, output: LoadOutputUtils): Callable[(Long, String)] = {
    return new Callable[(Long, String)] {
      override def call(): (Long, String) = {
        // TODO: done with file fregment
        log.info(s"running a task! ${file.getName}, ${begin}-${end} key position:${keyPos}")
        val length = (end - begin).toInt

        try {
          val n = LoadUtils.handle(file, begin.toInt, length, keyPos, output)
          lines.addAndGet(n)
          return (n,null)
        } catch {
          case ex: Throwable =>
            return (-1L, ex.getMessage)
        }
      }
    }
  }

  private def step2Decorator(handler: MySQLServerConnectionHandler): Unit = scala.concurrent.Future {
    val files = FileUtils.listFiles(new File(localDir), new IOFileFilter {
      override def accept(file: File): Boolean = file.getName.startsWith("loading_")
      override def accept(dir: File, name: String): Boolean = name.startsWith("loading_")
    },null)

    val partitionBeginPos = "loading_".length + tableName.length + 1

    val filesFolded = new util.HashMap[String, util.ArrayList[String]]
    files.foreach { f =>
      val pos = StringUtils.indexOf(f.getName, '_', partitionBeginPos)
      if ( pos != StringUtils.INDEX_NOT_FOUND ) {
        val partition = StringUtils.substring(f.getName, partitionBeginPos, pos)
        if ( !filesFolded.containsKey(partition) )
          filesFolded.put(partition, new util.ArrayList[String])

        filesFolded.get(partition).add(FilenameUtils.normalize(f.getPath, true))
      }
    }
    val pool = ExecutorServiceUtils.newFixedPool(concurrncyDegree, "loading-step-2")
    val futures = new util.ArrayList[Future[(Long, String)]]

    // 实际工作
    filesFolded.foreach { f =>
      futures.add(pool.submit(createFutureTask2(f._1, f._2)))
    }
    // 3. 执行任务
    pool.shutdown
    pool.awaitTermination(Long.MaxValue, TimeUnit.SECONDS)
    var isError = false
    var lines = 0L
    futures.foreach{f =>
      f.get match {
        case (n, msg) if n < 0 =>
          if ( !isError ) {
            isError = true
            handler.write(new ErrorMessage(n.toInt, msg))
          }
        case (n, _) =>
          lines += n
      }
    }
    if ( !isError )
        handler.write(new OkMessage(lines, 0L,0,0, null))
  }

  private def getLoadSql(targetTable: String, file: String): String = {
    log.info(s"loading to ${targetTable} from file: ${file}......")
    val buffer = new StringBuffer

    buffer.append("LOAD DATA LOCAL INFILE '").append(file).append("' INTO TABLE ").append(targetTable).
      append(" CHARACTER SET ").append(ScadbConfig.conf.busi.charset).
      append(" FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"'")
    buffer.toString
  }

  private def executeSql(pool: Connection, sql: String, promise: scala.concurrent.Promise[(Long, String)], targetTable: String, lines: Long, files: util.List[String]):Unit = {
    pool.sendQuery(sql, null).onComplete {
      case Success(v) =>
        if ( files.size <= 0 ) {
          promise trySuccess(lines + v.rowsAffected, null)
        } else {
          val file = files.remove(0)
          val loadSql = getLoadSql(targetTable,file)
          executeSql(pool, loadSql, promise, targetTable, lines + v.rowsAffected, files)
        }
      case Failure(e) =>
        log.error("send query {} error: {} {} !", sql, e.getMessage, "")
        promise trySuccess (-1L, e.getMessage)
    }
  }

  private def createFutureTask2(partition: String, files: util.List[String]): Callable[(Long, String)] = {
    return new Callable[(Long, String)] {
      override def call(): (Long, String) = {
        // TODO: done with file fregment
        log.info("running load task for partition [{}]!", partition)
        val promise = scala.concurrent.Promise[(Long, String)]
        val lines = 0L
        val (_, mysql, prefix) = ScadbConfig.conf.getServer(partition, table.mysqls)

        val targetTable = if ( prefix == null ) tableName + "_"  + partition else prefix + "." + tableName + "_" + partition

        if ( files.size <= 0 ) {
          promise trySuccess(lines, null)
        } else {
          val pool = ScadbMySQLPools.getPool(mysql, false).getOrElse(null)
          if (pool == null) {
            log.error("cannot find server:{} ==== is null", mysql)
            promise trySuccess(-1L, "get inner pool of [" + mysql + "] is null")
          } else {
            val loadSql = getLoadSql(targetTable, files.remove(0))
            executeSql(pool, loadSql, promise, targetTable, 0, files)
          }
        }
        val value = scala.concurrent.Await.result(promise.future, Duration.Inf)
        value
      }
    }
  }
}

