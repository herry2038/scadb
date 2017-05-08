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
package org.herry2038.scadb.db.general

import org.herry2038.scadb.db.{RowData, ResultSet}
import org.herry2038.scadb.util.Log

import collection.mutable.ArrayBuffer


object MutableResultSet {
  val log = Log.get[MutableResultSet[Nothing]]
}

class MutableResultSet[T <: ColumnData](
                        val columnTypes: IndexedSeq[T]) extends ResultSet {

  private val rows = new ArrayBuffer[RowData]()
  private val columnMapping: Map[String, Int] = this.columnTypes.indices.map(
    index =>
      ( this.columnTypes(index).name, index ) ).toMap
    

  val columnNames : IndexedSeq[String] = this.columnTypes.map(c => c.name)

  override def length: Int = this.rows.length

  override def apply(idx: Int): RowData = this.rows(idx)

  def addRow( row : Seq[Any] ) {
    val realRow = new ArrayRowData( columnTypes.size, this.rows.size, this.columnMapping )
    var x = 0
    while ( x < row.size ) {
      realRow(x) = row(x)
      x += 1
    }
    this.rows += realRow
  }

}