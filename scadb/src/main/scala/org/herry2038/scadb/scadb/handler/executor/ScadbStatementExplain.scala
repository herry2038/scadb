package org.herry2038.scadb.scadb.handler.executor

import org.herry2038.scadb.db.general.{ArrayRowData, MutableResultSet}
import org.herry2038.scadb.mysql.message.server.ColumnDefinitionMessage
import org.herry2038.scadb.mysql.server.MySQLServerConnectionHandler
import org.herry2038.scadb.scadb.conf.ScadbMySQLPools
import org.herry2038.scadb.util.Log

import scala.util.{Failure, Success}
import org.herry2038.scadb.db.util.ExecutorServiceUtils.CachedExecutionContext

class ScadbStatementExplain(handler: MySQLServerConnectionHandler,statement: ScadbStatement) extends ScadbStatement(handler, 0, -1) {
  val log = Log.get[ScadbStatementExplain]
  override def execute(): Int = {
    if ( statement.isInstanceOf[ScadbStatementSingle] ) {
      val statementSingle = statement.asInstanceOf[ScadbStatementSingle]
      val statementNew = new ScadbStatementSingle(handler, "Explain " + statementSingle.sql, statementSingle.server, 0, -1)
      executeInServr(statementSingle.server, "Explain " + statementSingle.sql)
    } else {
      0
    }
  }

  def executeInServr(server: String, sql: String): Int = {
    val pool = ScadbMySQLPools.getPool(server,false).getOrElse{
      log.error("cannot find server:{}",server)
      error(-1,"cannot get inner pool of [" + server + "]")
      return 0
    }
    if ( pool == null ) {
      log.error("cannot find server:{} ==== is null",server)
      error(-1,"get inner pool of [" + server + "] is null")
      return 0
    }
    pool.sendQuery(sql).onComplete{
      case Success(v) =>
        val rs = v.rows.get.asInstanceOf[MutableResultSet[ColumnDefinitionMessage]]
        val row0 = rs(0).asInstanceOf[ArrayRowData]
        row0.update(rs.columnNames.size-1, row0(rs.columnNames.size-1) + "\npool:" + server + ",rewrite sql:" + sql)
        //val value = new Array[Any](rs.columnNames.size)
        //value(rs.columnNames.size-1) = "pool:" + server + ",rewrite sql:" + sql
        //rs.addRow(value)
        write(v)
      case Failure(e) =>
        log.error("send query {} error: {} {} !", sql, e.getMessage,"")
        error(-1,e.getMessage)
    }
    0
  }
}
