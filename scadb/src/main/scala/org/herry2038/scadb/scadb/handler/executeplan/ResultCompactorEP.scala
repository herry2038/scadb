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
package org.herry2038.scadb.scadb.handler.executeplan

import java.util

import org.herry2038.scadb.db.general.{ColumnData, MutableResultSet, ArrayRowData}
import org.herry2038.scadb.mysql.codec.DecoderRegistry
import org.herry2038.scadb.mysql.column.ColumnTypes
import org.herry2038.scadb.mysql.message.server._
import org.herry2038.scadb.db.util.ByteBufferUtils
import MergeSorter.MergeSortor
import ResultCompactorEP.{CPkSort, Having, FieldExecutorType}
import com.alibaba.druid.sql.ast.SQLExpr
import com.alibaba.druid.sql.ast.expr.{SQLBinaryOpExpr, SQLBinaryOperator}
import org.herry2038.scadb.db.{RowData, QueryResult}
import io.netty.util.CharsetUtil
import org.herry2038.scadb.mysql.server.ResultCompactor
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._


/**
 * Created by Administrator on 2016/8/23.
 */
class ResultCompactorEP extends ResultCompactor {
  import ResultCompactorEP.FieldExecutorType._
  val fieldExecutorTypes = new util.ArrayList[FieldExecutorType]
  var orderByAggregate = false
  var haveGroupBy = false
  var limit = 0
  var havings: util.List[Having] = null

  private var currentProjectSize: Int = _
  private var orderByAddedSize: Int = _
  private var avgOffset: Int = _
  private var orderByPos: util.List[Int] = _
  private var groupByPos: util.List[Int] = _
  private val finalData = ArrayBuffer[RowData]()
  private var rows = 0
  private var columnTypes: IndexedSeq[ColumnData] = _
  private var realColumnTypes = new ArrayBuffer[ColumnData]

  def setInfo(projectSize: Int, orderByAddedSize: Int, avgOffset: Int, orderByPos: util.List[Int], groupByPos: util.List[Int]) = {
    this.currentProjectSize = projectSize
    this.orderByAddedSize = orderByAddedSize
    this.avgOffset = avgOffset
    this.orderByPos = orderByPos
    this.groupByPos = groupByPos
  }

  def setOrderByAggregate(orderByAggregate: Boolean) = this.orderByAggregate = orderByAggregate
  def setHaveGroupBy(haveGroupBy: Boolean) = this.haveGroupBy = haveGroupBy
  def setLimit(limit: Int) = this.limit = limit

  def add(fet: FieldExecutorType) = fieldExecutorTypes.add(fet)

  def setHaving(havings: util.List[Having]) = this.havings = havings

  def realColumns = currentProjectSize - orderByAddedSize - avgOffset
  override def compact(results: ArrayBuffer[QueryResult], out: util.List[Object]): Unit = {
    // FIELD 信息
    columnTypes = results(0).rows.get.asInstanceOf[MutableResultSet[ColumnData]].columnTypes

    val compactor = new CPkSort(columnTypes, groupByPos)
    val mergeSorter = new MergeSortor[RowData](compactor)

    val qrm = new QueryResultMessage(realColumns)
    out.add(qrm)

    val registry = new DecoderRegistry(CharsetUtil.UTF_8)

    var i = 0
    while ( i < currentProjectSize - orderByAddedSize ) {
      val (fieldName, dataType, dataTypeSize, step) = if ( fieldExecutorTypes.get(i) == AVG) {
        ("AVG", ColumnTypes.FIELD_TYPE_DOUBLE, 0L, 2 )
      } else {
        (columnTypes(i).name, columnTypes(i).dataType, columnTypes(i).dataTypeSize, 1)
      }

      val msg = new ColumnDefinitionMessage(
        "def", // catalog
        "", // schema
        "", // table
        "", // originalTable
        fieldName,
        fieldName,
        83,
        dataTypeSize,
        dataType,
        0,
        0,
        registry.binaryDecoderFor(ColumnTypes.FIELD_TYPE_VARCHAR, 83),
        registry.textDecoderFor(ColumnTypes.FIELD_TYPE_VARCHAR,83) )
      out.add(msg)

      realColumnTypes += msg
      i += step
    }

    val fieldEndMsg = new ColumnProcessingFinishedMessage(new EOFMessage(0,0))
    out.add(fieldEndMsg)

    // 行信息
    results.foreach{ r=>
      mergeSorter.addSorter(r.rows.get)
    }

    var current_data = mergeSorter.next
    if ( current_data.isEmpty ) {
      addEof(out)
      return
    }

    var row = mergeSorter.next
    while ( row.isDefined ) {
      if ( haveGroupBy && compactor.compare(current_data.get, row.get) == 0 ) {
        compactToCurrent(current_data.get, row.get, columnTypes)
      } else {
        if ( output1(current_data.get, columnTypes, out)) {
          return
        }
        current_data = row
      }
      row = mergeSorter.next
    }

    if ( output1(current_data.get, columnTypes, out) ) {
      return
    }

    if ( orderByAggregate ) {
      val lines = Math.min(finalData.size, limit)
      for ( i <- 0 until lines ) output0(finalData(i), out)
      //finalData.foreach(output0(_, out))
    }
    addEof(out)
  }

  private def output1(row: RowData, columnTypes: IndexedSeq[ColumnData], out: util.List[Object]): Boolean = {
    var pos = 0
    var i = 0

    val finalRow = new ArrayRowData(currentProjectSize - orderByAddedSize - avgOffset, finalData.size, null)

    while ( i < currentProjectSize - orderByAddedSize ) {
      if ( fieldExecutorTypes.get(i) == AVG ) {
        if ( row(i) == null || row(i+1) == null )
          finalRow.update(pos, null)
        else {
          val count = row(i + 1).asInstanceOf[String].toDouble.toInt
          if ( count == 0 )
            finalRow.update(pos, "0.000")
          else {
            val d = row(i).asInstanceOf[String].toDouble / count
            finalRow.update(pos, "%.3f".format(d))
          }
        }
        i += 1
      } else {
        finalRow.update(pos, row(i))
      }
      pos += 1 ; i += 1
    }

    val filtered = havings.foldLeft(false) { (filtered, having) =>
      filtered match {
        case true => true
        case false =>
          val compare_value = ResultCompactorEP.compare(row(having.pos), having.rightValue, columnTypes(having.pos).dataType)
          having.compareType match {
            case SQLBinaryOperator.Equality => compare_value != 0
            case SQLBinaryOperator.NotEqual => compare_value == 0
            case SQLBinaryOperator.GreaterThan => compare_value <= 0
            case SQLBinaryOperator.GreaterThanOrEqual => compare_value < 0
            case SQLBinaryOperator.LessThan => compare_value >= 0
            case SQLBinaryOperator.LessThanOrEqual => compare_value > 0
            case _ => true
          }
      }
    }

    if ( ! filtered ) {
      if ( orderByAggregate ) insertBuffer0(finalRow)
      else {
        output0(finalRow, out)
        rows += 1
        if ( rows >= limit ) {
          addEof(out)
          return true
        }
      }
    }
    false
  }

  private def insertBuffer0(finalRow: RowData): Unit = {
    if ( finalData.isEmpty ) {
      finalData += finalRow
      return
    }

    var min = 0
    var max = finalData.size - 1

    var mid = 0
    while ( min <= max ) {
      mid = ( min + max ) / 2

      val compareResult = compare_orderBy(finalRow, finalData(mid))

      if ( compareResult == 0 ) {
        finalData.insert( mid, finalRow)
        return
      }

      if ( compareResult > 0 )
        min = mid + 1
      else
        max = mid - 1
    }

    finalData.insert( max + 1, finalRow)
  }


  private def compare_orderBy(left: RowData, right: RowData): Long = orderByPos.foldLeft(0L){(result, pos) =>
    if ( result != 0 ) result
    else {
      val realPos = pos & 0xffff
      val order = pos & 0xffff0000

      val compare_result = ResultCompactorEP.compare(left(realPos), right(realPos), realColumnTypes(realPos).dataType)
      if ( order > 0 ) -compare_result else compare_result
    }
  }

  private def output0(row: RowData, out: util.List[Object]): Unit = {
    val resultSetRow = new ResultSetRowMessage
    row.foreach{f =>
      val buf = if ( f == null ) null else {
        val b = ByteBufferUtils.mysqlBuffer(1024)
        b.writeBytes(f.asInstanceOf[String].getBytes(CharsetUtil.UTF_8))
      }
      resultSetRow += buf
    }
    out.add(resultSetRow)
  }

  private def addEof(out: util.List[Object]): Unit = {
    val eofMessage = new EOFMessage(0,0)
    out.add(eofMessage)
  }

  private def compactToCurrent(current: RowData, row: RowData, columnTypes: IndexedSeq[ColumnData]): Unit = {
    (0 until fieldExecutorTypes.size).foreach{ i =>
      fieldExecutorTypes(i) match {
        case MIN => ResultCompactorEP.min_value(current.asInstanceOf[ArrayRowData], row(i), columnTypes(i).dataType, i)
        case MAX => ResultCompactorEP.max_value(current.asInstanceOf[ArrayRowData], row(i), columnTypes(i).dataType, i)
        case SUM | AVG | AVG_IGNORE => ResultCompactorEP.sum(current.asInstanceOf[ArrayRowData], row(i), columnTypes(i).dataType, i)
        case GROUP_BY | _  =>
      }
    }
  }
}

object ResultCompactorEP {
  class Having(val pos: Int, val compareType: SQLBinaryOperator, val rightValue: Number)
  object HavingCompareType extends Enumeration {
    type CompareType = Value//这里仅仅是为了将Enumration.Value的类型暴露出来给外界使用而已
    val EQUAL, GREAT, EQUAL_GREAT, LESS, LESS_GREAT = Value//在这里定义具体的枚举实例
  }

  object FieldExecutorType extends Enumeration {
    type FieldExecutorType = Value//这里仅仅是为了将Enumration.Value的类型暴露出来给外界使用而已
    val GROUP_BY, SUM, AVG, AVG_IGNORE, MIN, MAX = Value//在这里定义具体的枚举实例
  }

  def compare(col1: Any, col2: Any, fieldType: Int): Long = {
    fieldType match {
      case ColumnTypes.FIELD_TYPE_TINY | ColumnTypes.FIELD_TYPE_SHORT | ColumnTypes.FIELD_TYPE_LONG | ColumnTypes.FIELD_TYPE_LONGLONG | ColumnTypes.FIELD_TYPE_INT24 =>
        col1.asInstanceOf[String].toLong - (col2 match {
          case num: Number => num.longValue
          case _ => col2.asInstanceOf[String].toLong
        })
      case ColumnTypes.FIELD_TYPE_FLOAT | ColumnTypes.FIELD_TYPE_DOUBLE | ColumnTypes.FIELD_TYPE_DECIMAL | ColumnTypes.FIELD_TYPE_NEW_DECIMAL =>
        val d = col1.asInstanceOf[String].toDouble - (col2 match {
          case num: Number => num.longValue
          case _ => col2.asInstanceOf[String].toDouble
        })
        if ( d > 0.0001 ) 1L else if ( d < - 0.0001 ) -1 else 0 ;
      case _ =>
        return col1.asInstanceOf[String].compareTo(col2.asInstanceOf[String])
//      case MYSQL_TYPE_DATE:
//      case MYSQL_TYPE_TIME:
//      case MYSQL_TYPE_DATETIME:
//      case MYSQL_TYPE_STRING:
//      case MYSQL_TYPE_VAR_STRING:
//      case MYSQL_TYPE_BLOB:
//      case MYSQL_TYPE_TINY_BLOB:
//      case MYSQL_TYPE_MEDIUM_BLOB:
//      case MYSQL_TYPE_LONG_BLOB:
    }
  }


  class CPkSort(val fieldTypes: IndexedSeq[ColumnData], val groupBys: util.List[Int]) extends MergeSorter.Compare[RowData] {
    def compare(rowData1: RowData, rowData2: RowData): Long = {
      for (i <- 0 until groupBys.size) {
        val groupBy = groupBys.get(i)
        val pos = groupBy & 0xffff
        val direction = ( (groupBy & 0x10000) != 0 )

        val compare_value = ResultCompactorEP.compare(rowData1.apply(pos), rowData2.apply(pos), fieldTypes(pos).dataType)
        if ( compare_value != 0 ) {
          return if ( direction ) -compare_value else compare_value
        }
      }
      return 0
    }
  }

  def min_value(current: ArrayRowData, row: Any, fieldType: Int, pos: Int): Unit = {
      if ( compare(current(pos), row, fieldType) > 0 )
        current.update(pos, row)
  }

  def max_value(current: ArrayRowData, row: Any, fieldType: Int, pos: Int): Unit = {
    if ( compare(current(pos), row, fieldType) < 0 )
      current.update(pos, row)
  }

  def sum(current: ArrayRowData, row_input: Any, fieldType: Int, pos: Int): Unit = {
    val row = if ( row_input == null ) "0" else row_input.asInstanceOf[String]
    val currentPosValue = if ( current(pos) == null ) "0" else current(pos).asInstanceOf[String]

    fieldType match {
      case ColumnTypes.FIELD_TYPE_TINY | ColumnTypes.FIELD_TYPE_SHORT | ColumnTypes.FIELD_TYPE_LONG | ColumnTypes.FIELD_TYPE_LONGLONG | ColumnTypes.FIELD_TYPE_INT24 =>
        current.update(pos, (currentPosValue.toLong + row.toLong).toString)
      case ColumnTypes.FIELD_TYPE_FLOAT | ColumnTypes.FIELD_TYPE_DOUBLE | ColumnTypes.FIELD_TYPE_DECIMAL | ColumnTypes.FIELD_TYPE_NEW_DECIMAL =>
        val d = currentPosValue.toDouble + row.toDouble
        current.update(pos, "%.3f".format(d))
      case _ =>
        val d = currentPosValue.toDouble + row.toDouble
        current.update(pos, "%.3f".format(d))
    }
  }
  /*
  object FieldExecutorType {
    final val GROUP_BY = 1
    final val SUM = 2
    final val AVT = 4
    final val AVT_IGNORE = 8
    final val MIN = 16
    final val MAX = 32
  }
  */
}
