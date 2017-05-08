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
package org.herry2038.scadb.db.column

import java.sql.Timestamp
import java.util.{Calendar, Date}
import org.herry2038.scadb.db.exceptions.DateEncoderNotAvailableException
import org.joda.time._
import org.joda.time.format.DateTimeFormatterBuilder

object TimestampEncoderDecoder {
  val BaseFormat = "yyyy-MM-dd HH:mm:ss"
  val MillisFormat = ".SSSSSS"
  val Instance = new TimestampEncoderDecoder()
}

class TimestampEncoderDecoder extends ColumnEncoderDecoder {

  import TimestampEncoderDecoder._

  private val optional = new DateTimeFormatterBuilder()
    .appendPattern(MillisFormat).toParser
  private val optionalTimeZone = new DateTimeFormatterBuilder()
    .appendPattern("Z").toParser

  private val builder = new DateTimeFormatterBuilder()
    .appendPattern(BaseFormat)
    .appendOptional(optional)
    .appendOptional(optionalTimeZone)

  private val timezonedPrinter = new DateTimeFormatterBuilder()
    .appendPattern(s"${BaseFormat}${MillisFormat}Z").toFormatter

  private val nonTimezonedPrinter = new DateTimeFormatterBuilder()
    .appendPattern(s"${BaseFormat}${MillisFormat}").toFormatter

  private val format = builder.toFormatter

  def formatter = format

  override def decode(value: String): Any = {
    formatter.parseLocalDateTime(value)
  }

  override def encode(value: Any): String = {
    value match {
      case t: Timestamp => this.timezonedPrinter.print(new DateTime(t))
      case t: Date => this.timezonedPrinter.print(new DateTime(t))
      case t: Calendar => this.timezonedPrinter.print(new DateTime(t))
      case t: LocalDateTime => this.nonTimezonedPrinter.print(t)
      case t: ReadableDateTime => this.timezonedPrinter.print(t)
      case _ => throw new DateEncoderNotAvailableException(value)
    }
  }

}
