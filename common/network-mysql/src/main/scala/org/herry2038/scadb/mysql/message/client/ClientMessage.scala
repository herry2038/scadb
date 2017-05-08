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

package org.herry2038.scadb.mysql.message.client

import org.herry2038.scadb.db.KindedMessage

object ClientMessage {

  final val ClientProtocolVersion = 0x09 // COM_STATISTICS
  final val Quit = 0x01 // COM_QUIT
  final val Query = 0x03 // COM_QUERY
  final val InitDb = 0x02
  final val Ping = 14
  final val PreparedStatementPrepare = 0x16 // COM_STMT_PREPARE
  final val PreparedStatementExecute = 0x17 // COM_STMT_EXECUTE
  final val PreparedStatementSendLongData = 0x18 // COM_STMT_SEND_LONG_DATA
  final val AuthSwitchResponse = 0xfe // AuthSwitchRequest
  final val LoadData = 258           // load local data 数据
}

class ClientMessage ( val kind : Int ) extends KindedMessage