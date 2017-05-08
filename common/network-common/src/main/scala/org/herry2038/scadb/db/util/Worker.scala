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
package org.herry2038.scadb.db.util

import java.util.concurrent.ExecutorService
import org.herry2038.scadb.util.Log

import scala.concurrent.{ExecutionContextExecutorService, ExecutionContext}

object Worker {
  val log = Log.get[Worker]

  def apply() : Worker = apply(ExecutorServiceUtils.newFixedPool(1, "db-async-worker"))

  def apply( executorService : ExecutorService ) : Worker = {
    new Worker(ExecutionContext.fromExecutorService( executorService ))
  }

}

class Worker( val executionContext : ExecutionContextExecutorService ) {

  import Worker.log

  def action(f: => Unit) {
    this.executionContext.execute(new Runnable {
      def run() {
        try {
          f
        } catch {
          case e : Exception => {
            log.error("Failed to execute task %s".format(f), e)
          }
        }
      }
    })
  }

  def shutdown {
    this.executionContext.shutdown()
  }

}
