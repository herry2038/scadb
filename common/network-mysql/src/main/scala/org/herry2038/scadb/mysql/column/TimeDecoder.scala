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
package org.herry2038.scadb.mysql.column

import org.herry2038.scadb.db.column.ColumnDecoder
import scala.concurrent.duration._

object TimeDecoder extends ColumnDecoder {

  final val Hour = 1.hour.toMillis

  override def decode(value: String): Duration = {

    val pieces = value.split(':')

    val secondsAndMillis = pieces(2).split('.')

    val parts = if ( secondsAndMillis.length == 2 ) {
      (secondsAndMillis(0).toInt,secondsAndMillis(1).toInt)
    } else {
      (secondsAndMillis(0).toInt,0)
    }

    val hours = pieces(0).toInt
    val minutes = pieces(1).toInt

    hours.hours + minutes.minutes + parts._1.seconds + parts._2.millis
  }

}