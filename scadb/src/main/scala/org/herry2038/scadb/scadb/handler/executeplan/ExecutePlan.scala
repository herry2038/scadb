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

import com.alibaba.druid.sql.ast.{SQLOrderBy, SQLExpr, SQLOrderingSpecification}
import com.alibaba.druid.sql.ast.expr._
import com.alibaba.druid.sql.ast.statement.{SQLExprTableSource, SQLSelectItem, SQLSelectStatement}
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock
import org.herry2038.scadb.conf.algorithm.Algorithm
import org.herry2038.scadb.mysql.codec.MessageTrack
import org.herry2038.scadb.mysql.server.MySQLServerConnectionHandler
import org.herry2038.scadb.scadb.conf.ScadbConfig
import org.herry2038.scadb.scadb.handler.executor.{ScadbStatement, ScadbStatementParallelMulti}
import scala.collection.JavaConversions._

class ExecutePlan(val statement: SQLSelectStatement, val tableName: String,
                  val startTime: Long, val requestType: Int,
                  val rule: Algorithm, val handler: MySQLServerConnectionHandler) {
  import ResultCompactorEP._

  val queryBlock = statement.getSelect.getQuery.asInstanceOf[MySqlSelectQueryBlock]

  private val avgPoses = new util.ArrayList[Int] // 记录AVG的位置
  val orderByPrjPos = new util.ArrayList[Int]
  val groupByPrjPos = new util.ArrayList[Int]

  val orderByToBeAdd = new util.ArrayList[Int]()

  var orderByAggregate: Boolean = false
  var havingGroupBy: Boolean = queryBlock.getGroupBy != null

  val havings = new util.ArrayList[Having]
  var isDistinct: Boolean = false
  var limit: Int = 0
  var offset: Int = 0

  var realProjectionSize = queryBlock.getSelectList.size
  var orderByAddedProjectionSize = 0

  val resultCompareExecutePlan = new ResultCompactorEP

  private def formatOrderbyPos(pos: Int): Int = {
    var i = 0
    while ( i < avgPoses.size && pos > avgPoses.get(i) ) i = i + 1;
    pos + i
  }
  def checkProjection(): Unit = {

    //if ( queryBlock.getGroupBy == null && queryBlock.getOrderBy == null ) return

    queryBlock.getSelectList.foreach{ item =>
      if ( item.getExpr.isInstanceOf[SQLAllColumnExpr] ) {
        throw new RuntimeException("cannot support * query in this kind of query!!!")
      } else if ( item.getExpr.isInstanceOf[SQLAggregateExpr] ) {
        havingGroupBy = true
      }
    }
  }

  def equateOrderByWithProjections(): Unit = {
    Option(queryBlock.getOrderBy).map(_.getItems().foreach(_=>orderByPrjPos.add(-1)))
    Option(queryBlock.getGroupBy).map(_.getItems().foreach(_=>groupByPrjPos.add(-1)))

    if ( queryBlock.getOrderBy == null ) return

    var i = 0
    while ( i < queryBlock.getOrderBy.getItems.size ) {
      var bFind = ( orderByPrjPos.get(i) != -1 )
      if ( orderByPrjPos.get(i) == -1 ) {
        val orderByItem = queryBlock.getOrderBy.getItems.get(i)
        val orderExpression = orderByItem.getExpr.toString

        var j = 0
        while ( ! bFind && j < queryBlock.getSelectList.size ) {
          val selectItem = queryBlock.getSelectList.get(j)
          if ( orderExpression == selectItem.getExpr.toString || orderExpression == selectItem.getAlias ) {
            val mask = if ( orderByItem.getType() == SQLOrderingSpecification.ASC || orderByItem.getType() == null) 0x00000000 else 0x00010000
            orderByPrjPos.set(i, j | mask)
            bFind = true
          }
          j += 1
        }

        // 考虑加到 projection 中
        if ( ! bFind ) {
          if ( queryBlock.getGroupBy == null ) {
            // 没有groupby，则把该排序字段插入到projection中
            queryBlock.getSelectList.add(new SQLSelectItem(orderByItem.getExpr))
            val mask = if ( orderByItem.getType() == SQLOrderingSpecification.ASC ) 0x00000000 else 0x00010000
            orderByPrjPos.set(i, (queryBlock.getSelectList.size() - 1)| mask)
            realProjectionSize += 1
            orderByAddedProjectionSize += 1
            bFind = true
          } else {
            // 在groupby中找找，如果groupby中有，就增加到projection中
            Option(queryBlock.getGroupBy).map{ groupBys =>
              j = 0
              while ( ! bFind && j < groupBys.getItems.size ) {
                val groupBy = groupBys.getItems.get(j)
                if ( groupBy.toString == orderExpression ) {
                  queryBlock.getSelectList.add(new SQLSelectItem(orderByItem.getExpr))
                  val mask = if ( orderByItem.getType() == SQLOrderingSpecification.ASC ) 0x00000000 else 0x00010000
                  orderByPrjPos.set(i, (queryBlock.getSelectList.size() - 1)| mask)
                  realProjectionSize += 1
                  orderByAddedProjectionSize += 1
                  groupByPrjPos.set(j, queryBlock.getSelectList.size -1)
                  bFind = true
                }
                j += 1
              }

              // 如果没有找到，取消该排序功能
              if ( ! bFind ) {
                queryBlock.getOrderBy.getItems.remove(i)
                orderByPrjPos.remove(i)
              }
            }
          }
        }
      }

      if ( bFind ) i += 1
    }
  }

  def equateGroupByWithProjections(): Unit = {
    var i = 0
    while ( i < queryBlock.getGroupBy.getItems.size ) {
      var bFind = groupByPrjPos.get(i) != -1
      val groupByExpression = queryBlock.getGroupBy.getItems.get(i).toString
      if ( ! bFind ) {
        var j = 0
        while ( !bFind && j < queryBlock.getSelectList.size ) {
          val projection = queryBlock.getSelectList.get(j)
          if ( groupByExpression == projection.getExpr.toString || groupByExpression == projection.getAlias ) {
            groupByPrjPos.set(i, j)
            bFind = true
          }
          j += 1
        }
      }
      if ( ! bFind ) { // 如果没有知道对应的字段，则把该groupby 加入到projection中
        queryBlock.getSelectList.add(new SQLSelectItem(queryBlock.getGroupBy.getItems.get(i)))
        groupByPrjPos.set(i, queryBlock.getSelectList.size - 1)
        realProjectionSize += 1
        orderByAddedProjectionSize += 1 // 是否应该改成groupby
      }
      i += 1
    }
  }

  def createPlanSegmentFromTree(): Unit = {
    updateAggProjections() ;

    // m_vecGroupByPrjPos 以前是用来存储原SQL中的GROUP BY带的字段信息，
    // 从这里开始，他将重新定义：用于第一阶段的结果的排序顺序，也就是说用于归并排序处理所有的分区的查询结果

    // 如果没有GroupBy
    //  1. 必须带limit条件，并且limit 条件需要带到每个分片的查询上去
    //  2. 归并条件不需要添加额外的非order by字段

    // 如果有GroupBy
    //  1. 必须删除每个分片查询的limit条件
    //  2. 归并条件需要带上额外的非order by字段

    havingGroupBy = ! groupByPrjPos.isEmpty || queryBlock.getSelectList.exists(_.getExpr.isInstanceOf[SQLAggregateExpr])

    limit = 2147483647 ;
    if ( queryBlock.getLimit != null ) {
      limit = queryBlock.getLimit.getRowCount.toString().toInt

      if ( havingGroupBy ) {
        queryBlock.setLimit(null)
      }
    } else if ( ! havingGroupBy ) {
      throw new RuntimeException("table scan must have a limit!")
    }

    groupByPrjPos.clear

    if ( orderByAggregate ) {
      // 1. 清空原order by
      if ( resultCompareExecutePlan.fieldExecutorTypes.isEmpty )
        queryBlock.setOrderBy(null)
      // 2. 把所有group by 字段添加到order by中
      else {
        queryBlock.getOrderBy.getItems.clear()
        for (i <- 0 until resultCompareExecutePlan.fieldExecutorTypes.size ) {
          if ( resultCompareExecutePlan.fieldExecutorTypes.get(i) == ResultCompactorEP.FieldExecutorType.GROUP_BY ) {
            queryBlock.getOrderBy.addItem(queryBlock.getSelectList.get(i).getExpr, SQLOrderingSpecification.ASC)
            groupByPrjPos.add(i)
          }
        }
      }
    } else {
      // 把orderby中的数据，重整后放到Group by中，用于排序用
      var offsetTmp = 0

      /*
      for ( i <- 0 until orderByPrjPos.size ) {
        if ( resultCompareExecutePlan.fieldExecutorTypes.get(i + offsetTmp) == ResultCompactorEP.FieldExecutorType.AVG )
          offsetTmp += 1
        groupByPrjPos.add(orderByPrjPos.get(i) + offsetTmp )
      }
      */
      orderByPrjPos.foreach { pos =>
        val regularPos = formatOrderbyPos(pos)
        if ( resultCompareExecutePlan.fieldExecutorTypes.get(regularPos & 0xffff) == ResultCompactorEP.FieldExecutorType.GROUP_BY ) {
          groupByPrjPos.add(regularPos)
        }
      }

      // 如果有Group by，归并条件需要带上额外的非orderby字段
      if ( havingGroupBy ) {
        if ( !orderByToBeAdd.isEmpty ) {
          if ( queryBlock.getOrderBy == null ) queryBlock.setOrderBy(new SQLOrderBy())
          orderByToBeAdd.foreach { i =>
            queryBlock.getOrderBy.addItem(queryBlock.getSelectList.get(i).getExpr, SQLOrderingSpecification.ASC)
            groupByPrjPos.add(i)
          }
        }
      }
    }
  }

  def equateHavingWithProjections(): Unit = {
    // TODO: 暂时只支持count(b) and sum(a) > 2这种形势的having
    if ( queryBlock.getGroupBy.getHaving == null ) return

    doEquateHaving(queryBlock.getGroupBy.getHaving)

    //resultCompareExecutePlan.setHaving(havings)
    // 处理完就不在需要having了。
    queryBlock.getGroupBy.setHaving(null)
  }

  private def doEquateHaving(havingCond: SQLExpr) {
    if (  !havingCond.isInstanceOf[SQLBinaryOpExpr] ) {
      throw new RuntimeException("unsupported having clause!")
    }
    val having = havingCond.asInstanceOf[SQLBinaryOpExpr]
    if (having.getOperator == SQLBinaryOperator.GreaterThan || having.getOperator == SQLBinaryOperator.GreaterThanOrEqual ||
      having.getOperator == SQLBinaryOperator.LessThan || having.getOperator == SQLBinaryOperator.LessThanOrEqual || having.getOperator == SQLBinaryOperator.Equality) {
      doEquateOneHaving(having)
      return
    } else if (having.getOperator != SQLBinaryOperator.BooleanAnd) {
      throw new RuntimeException("cannot support this kind of having clause!")
    }

    doEquateHaving(having.getLeft())
    doEquateHaving(having.getRight())
  }

  private def doEquateOneHaving(having: SQLBinaryOpExpr): Unit = {
    val havingExpression = having.getLeft.toString
    var bFind = false
    var j = 0

    while ( ! bFind & j < queryBlock.getSelectList.size ) {
      val selectItem = queryBlock.getSelectList.get(j)
      if ( havingExpression == selectItem.getAlias || havingExpression == selectItem.getExpr.toString ) {
        if ( !selectItem.getExpr.isInstanceOf[SQLAggregateExpr] ) {
          throw new RuntimeException("only support this kind of having -> count(*) > 1 and sum(a) = 2!")
        }
        bFind = true
      }
    }

    if ( ! bFind ) {
      if ( !having.getLeft().isInstanceOf[SQLAggregateExpr]) {
        throw new RuntimeException("only support this kind of having -> count(*) > 1 and sum(a) = 2!")
      }

      queryBlock.getSelectList.add(new SQLSelectItem(having.getLeft))
      orderByAddedProjectionSize += 1
      realProjectionSize += 1
    }

    if ( ! having.getRight.isInstanceOf[SQLNumericLiteralExpr] ) {
      throw new RuntimeException("only support this kind of having -> count(*) > 1 and sum(a) = 2!")
    }

    val havingObj = new Having(j, having.getOperator, having.getRight.asInstanceOf[SQLNumericLiteralExpr].getNumber)
    havings.add(havingObj)
  }

  private def updateAggProjections(): Unit = {
    var groupByPos = 0
    var i = 0
    var colPos = 0

    while ( i < queryBlock.getSelectList.size ) {
      val item = queryBlock.getSelectList.get(i)

      //if (colPos > 0) strPrefixOfEveryProjection = "," else strPrefixOfEveryProjection = "SELECT ";


      if (item.getExpr.isInstanceOf[SQLAggregateExpr]) {
        if (!orderByAggregate) orderByAggregate = orderByPrjPos.exists { orderByPos => (orderByPos & 0xffff) == colPos }

        val aggregetExpr = item.getExpr.asInstanceOf[SQLAggregateExpr]
        if (aggregetExpr.getMethodName.equalsIgnoreCase("sum")) {
          resultCompareExecutePlan.add(ResultCompactorEP.FieldExecutorType.SUM)
        } else if (aggregetExpr.getMethodName.equalsIgnoreCase("count")) {
          resultCompareExecutePlan.add(ResultCompactorEP.FieldExecutorType.SUM)
        } else if (aggregetExpr.getMethodName.equalsIgnoreCase("avg")) {
          avgPoses.add(i-offset) // 把 avg的位置记录下来
          aggregetExpr.setMethodName("sum")

          val addedCountItem = new SQLAggregateExpr("count", aggregetExpr.getOption)
          addedCountItem.setParent(aggregetExpr.getParent)
          addedCountItem.setKeep(aggregetExpr.getKeep)
          addedCountItem.setOver(aggregetExpr.getOver)
          addedCountItem.setIgnoreNulls(aggregetExpr.isIgnoreNulls)
          addedCountItem.setWithinGroup(aggregetExpr.getWithinGroup)
          aggregetExpr.getArguments.foreach(addedCountItem.addArgument(_))

          queryBlock.getSelectList.add(i + 1, new SQLSelectItem(addedCountItem))

          resultCompareExecutePlan.add(ResultCompactorEP.FieldExecutorType.AVG)
          resultCompareExecutePlan.add(ResultCompactorEP.FieldExecutorType.AVG_IGNORE)
          i += 1
          offset += 1 // 偏移量增加量了，因为添加了一个额外的字段
          realProjectionSize += 1
        } else if (aggregetExpr.getMethodName.equalsIgnoreCase("max")) {
          resultCompareExecutePlan.add(ResultCompactorEP.FieldExecutorType.MAX)
        } else if (aggregetExpr.getMethodName.equalsIgnoreCase("min")) {
          resultCompareExecutePlan.add(ResultCompactorEP.FieldExecutorType.MIN)
        }
      } else {
        groupByPos += 1

        resultCompareExecutePlan.add(ResultCompactorEP.FieldExecutorType.GROUP_BY)

        if (!orderByPrjPos.exists { orderByPos => (orderByPos & 0xffff) == colPos }) orderByToBeAdd.add(i)
      }
      colPos += 1
      i += 1
    }

    /*  这块代码似乎没有意义了，因为having已经在前面被干掉了。
    if ( queryBlock.getGroupBy != null && queryBlock.getGroupBy.getHaving != null ) {
      updateHaving() ;
    }
    */
  }


  private def setPartition(partition: String, prefix: String): Unit = {
    statement.asInstanceOf[SQLSelectStatement].getSelect().getQuery().
      asInstanceOf[MySqlSelectQueryBlock].getFrom().
      asInstanceOf[SQLExprTableSource].getExpr.asInstanceOf[SQLIdentifierExpr].
      setName(if( prefix != null ) prefix + "." + tableName + "_" + partition else tableName + "_" + partition)
  }


  def createPlan(): ScadbStatement = {
    checkProjection()

    equateOrderByWithProjections()
    if ( queryBlock.getGroupBy != null ) {
      equateGroupByWithProjections()
      equateHavingWithProjections()
    }
    createPlanSegmentFromTree()

    resultCompareExecutePlan.setInfo(realProjectionSize, orderByAddedProjectionSize, offset, orderByPrjPos, groupByPrjPos)
    resultCompareExecutePlan.setHaving(havings)
    resultCompareExecutePlan.setOrderByAggregate(orderByAggregate)
    resultCompareExecutePlan.setLimit(limit)
    resultCompareExecutePlan.setHaveGroupBy(havingGroupBy)

    val sqls = new java.util.ArrayList[(String, String, MessageTrack)]
    rule.partitions(tableName, ScadbConfig.conf.busi.mysqls, ScadbConfig.conf.busi.dbPrefix, ScadbConfig.conf.busi.dbNums).foreach { i =>
     //setPartition(i._1, ScadbConfig.conf.busi.dbPrefix)
      statement.asInstanceOf[SQLSelectStatement].getSelect().getQuery().
        asInstanceOf[MySqlSelectQueryBlock].getFrom().
        asInstanceOf[SQLExprTableSource].getExpr.asInstanceOf[SQLIdentifierExpr].
        setName(i._2)
      sqls.add((i._3, statement.toString(), null))
    }
    new  ScadbStatementParallelMulti(handler,sqls, ScadbConfig.conf.parallelDegree,
      startTime, requestType,
      handler.isReadWriteSplit, resultCompareExecutePlan)
  }
}
