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

package org.herry2038.scadb.mysql.binary.encoder

import io.netty.buffer.ByteBuf
import org.herry2038.scadb.mysql.column.ColumnTypes
import org.joda.time.LocalTime

object LocalTimeEncoder extends BinaryEncoder {
  def encode(value: Any, buffer: ByteBuf) {
    val time = value.asInstanceOf[LocalTime]

    val hasMillis = time.getMillisOfSecond != 0

    if ( hasMillis ) {
      buffer.writeByte(12)
    } else {
      buffer.writeByte(8)
    }

    if ( time.getMillisOfDay > 0 ) {
      buffer.writeByte(0)
    } else {
      buffer.writeByte(1)
    }

    buffer.writeInt(0)

    buffer.writeByte(time.getHourOfDay)
    buffer.writeByte(time.getMinuteOfHour)
    buffer.writeByte(time.getSecondOfMinute)

    if ( hasMillis ) {
      buffer.writeInt(time.getMillisOfSecond * 1000)
    }

  }

  def encodesTo: Int = ColumnTypes.FIELD_TYPE_TIME
}
