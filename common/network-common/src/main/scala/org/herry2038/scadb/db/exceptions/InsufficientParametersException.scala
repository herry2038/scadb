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
package org.herry2038.scadb.db.exceptions

/**
 *
 * Raised when the user gives more or less parameters than the query takes. Each parameter is a ?
 * (question mark) in the query string. The count of ? should be the same as the count of items in the provided
 * sequence of parameters.
 *
 * @param expected the expected count of parameters
 * @param given the collection given
 */
class InsufficientParametersException( expected : Int, given : Seq[Any] )
  extends DatabaseException(
    "The query contains %s parameters but you gave it %s (%s)".format(expected, given.length, given.mkString(",")
    )
  )
