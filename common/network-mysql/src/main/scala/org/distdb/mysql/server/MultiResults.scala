package org.distdb.mysql.server

import com.github.mauricio.async.db.QueryResult

import scala.collection.mutable.ArrayBuffer

/**
 * Created by Administrator on 2016/8/26.
 */
class MultiResults(val resultCompactor: ResultCompactor = ResultCompactorCommon.resultCompactorCommon, val results: ArrayBuffer[QueryResult] = new ArrayBuffer[QueryResult]) {
  def add(result: QueryResult) = results += result
}
