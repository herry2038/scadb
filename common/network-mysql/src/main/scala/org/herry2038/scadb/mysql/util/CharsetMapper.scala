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

import org.herry2038.scadb.db.exceptions.DatabaseException
import org.herry2038.scadb.mysql.exceptions.CharsetMappingNotAvailableException
import java.nio.charset.Charset
import io.netty.util.CharsetUtil

object CharsetMapper {

  final val Binary = 63

  final val DefaultCharsetsByCharset = Map[Charset,Int](
    CharsetUtil.UTF_8 -> 83,
    CharsetUtil.US_ASCII -> 11,
    CharsetUtil.US_ASCII -> 65,
    CharsetUtil.ISO_8859_1 -> 3,
    CharsetUtil.ISO_8859_1 -> 69
  )

  final val DefaultCharsetsByString = Map[Charset,String](
    CharsetUtil.UTF_8 -> "utf8" ,
    CharsetUtil.US_ASCII -> "latin1"
  )

  final val DefaultCharsetsById = DefaultCharsetsByCharset.map { pair => (pair._2, pair._1.name()) }

  final val Instance = new CharsetMapper()

  def toString(charset: Charset, utf8mb4: Boolean): String = {
    if (utf8mb4)
      "utf8mb4"
    else
      DefaultCharsetsByString.getOrElse(charset, {
        throw new DatabaseException("There is no MySQL charset mapping name for the Java Charset %s".format(charset))
      })
  }
}

class CharsetMapper( charsetsToIntComplement : Map[Charset,Int] = Map.empty[Charset,Int] ) {

  private var charsetsToInt = CharsetMapper.DefaultCharsetsByCharset ++ charsetsToIntComplement

  def toInt(charset: Charset): Int = {
    charsetsToInt.getOrElse(charset, {
      throw new CharsetMappingNotAvailableException(charset)
    })
  }




}
