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

package org.herry2038.scadb.sentinel

import java.util.{TimerTask, Timer}
import java.util.concurrent.{TimeUnit, Executors}

object ScadbSentinel {

  def main(args: Array[String]) {
    SentinelConf.load


    val timerDetector = new Timer
    timerDetector.schedule(new TimerTask {
      override def run(): Unit = {
        SentinelDetectorService.detector
      }
    }, SentinelConf.detectorInterval, SentinelConf.detectorInterval)

    new Timer().schedule(new TimerTask {
      override def run(): Unit = {

      }
    }, SentinelConf.statisticsInterval, SentinelConf.statisticsInterval)
  }


}
