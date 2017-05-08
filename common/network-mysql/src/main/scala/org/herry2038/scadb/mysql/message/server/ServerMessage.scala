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
package org.herry2038.scadb.mysql.message.server

import org.herry2038.scadb.db.KindedMessage

object ServerMessage {

  final val ServerProtocolVersion = 10
  final val Error = -1
  final val LoadLocal = -5
  final val Ok = 0
  final val EOF = -2

  // these messages don't actually exist
  // but we use them to simplify the switch statements
  final val ColumnDefinition = 100
  final val ColumnDefinitionFinished = 101
  final val ParamProcessingFinished = 102
  final val ParamAndColumnProcessingFinished = 103
  final val Row = 104
  final val BinaryRow = 105
  final val PreparedStatementPrepareResponse = 106
  final val QueryResult = 107
}

class ServerMessage( val kind : Int ) extends KindedMessage
