package org.distdb.mysql.server

import com.github.mauricio.async.db.QueryResult

import scala.collection.mutable.ArrayBuffer

/**
 * Created by Administrator on 2016/8/26.
 */
trait ResultCompactor {
  def compact(results: ArrayBuffer[QueryResult], out: java.util.List[Object])
}
