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

import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.AbstractQueuedSynchronizer

class CountLatch(c: Int) {
  def this() = this(0)
  val sync = new Sync(c)

  def setCount(count: Int) = sync.setCount(count)

  def getCount = sync.count

  def await = sync.acquireSharedInterruptibly(-1)

  def await(timeout: Long, unit: TimeUnit) = sync.tryAcquireSharedNanos(-1, unit.toNanos(timeout))

  def countUp = sync.increment

  def countDown = sync.releaseShared(-1)

  def count = sync.count

  override def toString(): String = s"Latch(${sync.count}"

  class Sync(c: Int) extends AbstractQueuedSynchronizer {
    setCount(c)

    def count: Int = getState

    override protected def tryAcquireShared(ignored: Int) = if (count == 0) 1 else -1
    override protected def tryReleaseShared(ignored: Int): Boolean = {
      while (true ) {
        val current = count
        if ( current == 0 ) return false
        val decrement = current - 1
        if ( compareAndSetState(current, decrement)) return decrement == 0
      }
      false
    }

    def increment: Boolean = {
      while ( true ) {
        val current = count
        if ( current == 0 ) return false
        val increment = current + 1
        if ( compareAndSetState(current, increment)) return increment == 0
      }
      false
    }

    def setCount(count: Int) = setState(count)
  }


}
