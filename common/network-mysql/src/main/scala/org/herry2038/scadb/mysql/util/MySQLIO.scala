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

package org.herry2038.scadb.mysql.util

object MySQLIO {

  final val CLIENT_PROTOCOL_41 = 0x0200
  final val CLIENT_CONNECT_WITH_DB = 0x0008
  final val CLIENT_TRANSACTIONS = 0x2000
  final val CLIENT_MULTI_RESULTS = 0x200000
  final val CLIENT_LONG_FLAG = 0x0001
  final val CLIENT_LOCAL_FILES = 0x00080
  final val CLIENT_PLUGIN_AUTH = 0x00080000
  final val CLIENT_SECURE_CONNECTION = 0x00008000

}
