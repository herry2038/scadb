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
package org.herry2038.scadb.mysql.codec

import org.herry2038.scadb.mysql.message.server.{ColumnDefinitionMessage, PreparedStatementPrepareResponse}
import scala.collection.mutable.ArrayBuffer

class PreparedStatementHolder( val statement : String, val message : PreparedStatementPrepareResponse ) {

  val columns = new ArrayBuffer[ColumnDefinitionMessage]
  val parameters = new ArrayBuffer[ColumnDefinitionMessage]

  def statementId : Array[Byte] = message.statementId

  def needsParameters : Boolean = message.paramsCount != this.parameters.length

  def needsColumns : Boolean = message.columnsCount != this.columns.length

  def needsAny : Boolean = this.needsParameters || this.needsColumns

  def add( column : ColumnDefinitionMessage ) {
    if ( this.needsParameters ) {
      this.parameters += column
    } else {
      if ( this.needsColumns ) {
        this.columns += column
      }
    }
  }

  override def toString: String = s"PreparedStatementHolder($statement)"

}
