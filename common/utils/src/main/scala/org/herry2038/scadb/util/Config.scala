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
package org.herry2038.scadb.util

import java.io.IOException
import java.util.Properties
import java.util.concurrent.ConcurrentHashMap
import scala.util.Try
import scala.collection.JavaConversions._

class Config extends Logging {
  val configs = new ConcurrentHashMap[String, String]()
  def loadConfig(fileName: String): Unit = {
    val in = this.getClass.getResourceAsStream(fileName)
    if ( in == null ) return
    try {
      val prop = new Properties
      prop.load(in)
      prop.foreach{ kv=> configs.put(kv._1, kv._2)}
    } catch {
      case ex: IOException =>
        error(s"load config ${fileName} error!", ex)
    } finally {
      Try(in.close)
    }
  }

  private def sanitize(str: String): Option[String] = Option(str).map(_.trim)

  def getString(property: String): Option[String] = sanitize(configs.get(property))

  def getString(property: String, defaultValue: String = "") = sanitize(configs.get(property)).getOrElse(defaultValue)

  def getInt(property: String, defaultValue: Int = 0): Int = sanitize(configs.get(property)).map(_.toInt).getOrElse(defaultValue)

  def putString(property: String, value: String) = configs.put(property,value)

}

object Config {
  val instance = new Config
  def loadConfig(fileName: String): Unit = instance.loadConfig(fileName)
}

