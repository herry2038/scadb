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
package org.herry2038.scadb.db

import java.nio.charset.Charset
import scala.Predef._
import scala.{None, Option, Int}
import io.netty.util.CharsetUtil
import io.netty.buffer.AbstractByteBufAllocator
import io.netty.buffer.PooledByteBufAllocator
import scala.concurrent.duration._

object Configuration {
  val DefaultCharset = CharsetUtil.UTF_8
  val DefaultCharsetStr = "utf8"
  val Default = new Configuration("postgres")
}

/**
 *
 * Contains the configuration necessary to connect to a database.
 *
 * @param username database username
 * @param host database host, defaults to "localhost"
 * @param port database port, defaults to 5432
 * @param password password, defaults to no password
 * @param database database name, defaults to no database
 * @param charset charset for the connection, defaults to UTF-8, make sure you know what you are doing if you
 *                change this
 * @param maximumMessageSize the maximum size a message from the server could possibly have, this limits possible
 *                           OOM or eternal loop attacks the client could have, defaults to 16 MB. You can set this
 *                           to any value you would like but again, make sure you know what you are doing if you do
 *                           change it.
 */

case class Configuration(username: String,
                         host: String = "localhost",
                         port: Int = 5432,
                         password: Option[String] = None,
                         database: Option[String] = None,
                         //charset: String = Configuration.DefaultCharsetStr,
                         charset: Charset = Configuration.DefaultCharset,
                         autoCommit: Boolean = false,
                         utf8mb4: Boolean = false,
                         returnStr: Boolean = false,
                         maximumMessageSize: Int = 16777216,
                         allocator: AbstractByteBufAllocator = PooledByteBufAllocator.DEFAULT,
                         connectTimeout: Duration = 5.seconds,
                         testTimeout: Duration = 5.seconds
                          )
