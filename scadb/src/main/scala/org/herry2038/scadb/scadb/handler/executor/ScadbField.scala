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
package org.herry2038.scadb.scadb.handler.executor

class ScadbField (
	val table: String,
	val name : String,
	val length : Long,
	val ftype : Byte,
	val flags : Int
)


object ScadbField {
	val DP_TYPE_DECIMAL = 0 
	val DP_TYPE_TINY = 1
	val DP_TYPE_SHORT = 2
	val DP_TYPE_LONG = 3
	val DP_TYPE_FLOAT = 4 
	val DP_TYPE_DOUBLE = 5
	val DP_TYPE_NULL = 6
	val DP_TYPE_TIMESTAMP = 7
	val DP_TYPE_LONGLONG = 8
	val DP_TYPE_INT24 = 9
	val DP_TYPE_DATE = 10
	val DP_TYPE_TIME = 11
	val DP_TYPE_DATETIME = 12
	val DP_TYPE_YEAR = 13
	val DP_TYPE_NEWDATE = 14
	val DP_TYPE_VARCHAR = 16
	val DP_TYPE_BIT = 17
	val DP_TYPE_NEWDECIMAL = 246
	val DP_TYPE_ENUM = 247
	val DP_TYPE_SET = 248
	val DP_TYPE_TINY_BLOB = 249
	val DP_TYPE_MEDIUM_BLOB = 250
	val DP_TYPE_LONG_BLOB = 251
	val DP_TYPE_BLOB = 252
	val DP_TYPE_VAR_STRING = 253
	val DP_TYPE_STRING = 254
	val DP_TYPE_GEOMETRY = 255
}
