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

import org.herry2038.scadb.db.exceptions.DateEncoderNotAvailableException
import org.joda.time.format.DateTimeFormat
import org.joda.time.{ReadablePartial, LocalDate}

object DateEncoderDecoder extends ColumnEncoderDecoder {

  private val ZeroedDate = "0000-00-00"

  private val formatter = DateTimeFormat.forPattern("yyyy-MM-dd")

  override def decode(value: String): LocalDate =
    if ( ZeroedDate == value ) {
      null
    } else {
      this.formatter.parseLocalDate(value)
    }

  override def encode(value: Any): String = {
    value match {
      case d: java.sql.Date => this.formatter.print(new LocalDate(d))
      case d: ReadablePartial => this.formatter.print(d)
      case _ => throw new DateEncoderNotAvailableException(value)
    }
  }

}
