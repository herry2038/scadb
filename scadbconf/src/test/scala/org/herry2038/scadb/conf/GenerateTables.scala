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
package org.herry2038.scadb.conf

object GenerateTables {
  def main(args: Array[String]) {
    0 until 100 foreach (i=>
      //println(s"CREATE TABLE `a_${i}` (`id` BIGINT(20) NOT NULL DEFAULT '0',	`name` VARCHAR(50) NULL DEFAULT NULL COLLATE 'utf8_bin',	`t` DATETIME NULL DEFAULT NULL,	`b` BLOB NULL,	PRIMARY KEY (`id`)) COLLATE='utf8_bin' ENGINE=InnoDB ROW_FORMAT=DEFAULT ;")
      //println(s"TRUNCATE TABLE `a_${i}`;")
      println(s"select count(*) from a_${i};")
    )
  }
}
