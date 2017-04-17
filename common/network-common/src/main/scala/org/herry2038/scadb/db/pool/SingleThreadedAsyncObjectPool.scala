
package org.herry2038.scadb.db.pool

import java.util.concurrent.atomic.AtomicLong
import java.util.{TimerTask, Timer}
import org.herry2038.scadb.db.util.{Worker, Log}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Promise, Future}
import scala.util.{Failure, Success}

object SingleThreadedAsyncObjectPool {
  val Counter = new AtomicLong()
  val log = Log.get[SingleThreadedAsyncObjectPool[Nothing]]
}

/**
 *
 * Implements an [[AsyncObjectPool]] using a single thread from a
 * fixed executor service as an event loop to cause all calls to be sequential.
 *
 * Once you are done with this object remember to call it's close method to clean up the thread pool and
 * it's objects as this might prevent your application from ending.
 *
 * @param factory
 * @param configuration
 * @tparam T type of the object this pool holds
 */

class SingleThreadedAsyncObjectPool[T](val name: String,
                                        factory: ObjectFactory[T],
                                        configuration: PoolConfiguration
                                        ) extends AsyncObjectPool[T] {

  import SingleThreadedAsyncObjectPool.{Counter, log}

  private val mainPool = Worker()
  implicit val executorContext = mainPool.executionContext

  private val poolables = new ArrayBuffer[PoolableHolder[T]](configuration.maxObjects)
  private val checkouts = new ArrayBuffer[T](configuration.maxObjects)
  private val waitQueue = new ArrayBuffer[Promise[T]](configuration.maxQueueSize)
  private val timer = new Timer("async-object-pool-timer-" + Counter.incrementAndGet(), true)
  timer.scheduleAtFixedRate(new TimerTask {
    def run() {
      mainPool.action {
        testObjects
      }
    }
  }, configuration.validationInterval, configuration.validationInterval)

  private var closed = false

  /**
   *
   * Asks for an object from the pool, this object should be returned to the pool when not in use anymore.
   *
   * @return
   */

  def take: Future[T] = {

    if (this.closed) {
      return Promise.failed(new PoolAlreadyTerminatedException).future
    }

    val promise = Promise[T]()
    this.checkout(promise)
    promise.future
  }

  /**
   *
   * Returns an object to the pool. The object is validated before being added to the collection
   * of available objects to make sure we have a usable object. If the object isn't valid it's discarded.
   *
   * @param item
   * @return
   */

  def giveBack(item: T): Future[AsyncObjectPool[T]] = {
    val promise = Promise[AsyncObjectPool[T]]()
    this.mainPool.action {
      this.checkouts -= item
      this.factory.validate(item) match {
        case Success(item) => {
          this.addBack(item, promise)
        }
        case Failure(e) => {
          this.factory.destroy(item)
          this.addBack(null.asInstanceOf[T], promise)
          promise.failure(e)
        }
      }
    }

    promise.future
  }

  def isFull: Boolean = this.poolables.isEmpty && this.checkouts.size == configuration.maxObjects

  def  close: Future[AsyncObjectPool[T]] = {
    val promise = Promise[AsyncObjectPool[T]]()

    this.mainPool.action {
      if (!this.closed) {
        try {
          this.timer.cancel()
          this.mainPool.shutdown
          this.closed = true
          (this.poolables.map(i => i.item) ++ this.checkouts).foreach(item => factory.destroy(item))
          promise.success(this)
        } catch {
          case e: Exception => promise.failure(e)
        }
      } else {
        promise.success(this)
      }
    }

    promise.future
  }

  def availables: Traversable[T] = this.poolables.map(item => item.item)

  def inUse: Traversable[T] = this.checkouts

  def queued: Traversable[Promise[T]] = this.waitQueue

  def isClosed: Boolean = this.closed

  /**
   *
   * Adds back an object that was in use to the list of poolable objects.
   *
   * @param item
   * @param promise
   */

  private def addBack(item: T, promise: Promise[AsyncObjectPool[T]]) {
    if ( item != null ) { // 2016.12.17 有可能连接故障了，不能还回来。
      this.poolables += new PoolableHolder[T](item)
    }
    if (!this.waitQueue.isEmpty) {
      this.checkout(this.waitQueue.remove(0))
    }
    if ( item != null ) {
      promise.success(this)
    }
  }

  /**
   *
   * Enqueues a promise to be fulfilled in the future when objects are sent back to the pool. If
   * we have already reached the limit of enqueued objects, fail the promise.
   *
   * @param promise
   */

  private def enqueuePromise(promise: Promise[T]) {
    if (this.waitQueue.size >= configuration.maxQueueSize) {
      log.info("There are no objects available and the waitQueue is full")
      val exception = new PoolExhaustedException("There are no objects available and the waitQueue is full")
      exception.fillInStackTrace()
      promise.failure(exception)
    } else {
      this.waitQueue += promise
    }
  }

  private def checkout(promise: Promise[T]) {
    this.mainPool.action {
      if (this.isFull) {
        this.enqueuePromise(promise)
      } else {
        this.createOrReturnItem(promise)
      }
    }
  }

  /**
   *
   * Checks if there is a poolable object available and returns it to the promise.
   * If there are no objects available, create a new one using the factory and return it.
   *
   * @param promise
   */

  private def createOrReturnItem(promise: Promise[T]) {
    if (this.poolables.isEmpty) {
      try {
        val itemFuture = this.factory.create(Counter.incrementAndGet())
        itemFuture map { item =>
          this.checkouts += item
          promise.success(item)
        } recover { // 添加日期: 20160302 原因，有可能连接失败，这时候需要能够捕获异常。
          case e: Exception => promise.failure(e)
        }
      } catch {
        case e: Exception => promise.failure(e)
      }
    } else {
      val item = this.poolables.remove(0).item
      this.checkouts += item
      promise.success(item)
    }
  }

  override def finalize() {
    this.close
  }

  /**
   *
   * Validates pooled objects not in use to make sure they are all usable, great if
   * you're holding onto network connections since you can "ping" the destination
   * to keep the connection alive.
   *
   */

  override def testObjects {
    val removals = new ArrayBuffer[PoolableHolder[T]]()
    this.poolables.foreach {
      poolable =>
        this.factory.test(poolable.item) match {
          case Success(item) => {
            if (poolable.timeElapsed > configuration.maxIdle) {
              log.info("Connection was idle for {}, maxIdle is {}, removing it", poolable.timeElapsed, configuration.maxIdle)
              removals += poolable
              factory.destroy(poolable.item)
            }
          }
          case Failure(e) => {
            log.error("Failed to validate object", e)
            removals += poolable
          }
        }
    }
    this.poolables --= removals
  }

  private class PoolableHolder[T](val item: T) {
    val time = System.currentTimeMillis()

    def timeElapsed = System.currentTimeMillis() - time
  }


  override def status = {
    new StringBuffer().append(configuration.maxObjects).append('-').append(configuration.idleObjects)
      .append('-').append(configuration.maxQueueSize).append('-')
      .append(this.poolables.size).append('-')
      .append(this.checkouts.size).append('-')
      .append(this.waitQueue.size).toString
  }
}
