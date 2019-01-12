package spaa19

import java.util.concurrent.Phaser
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater
import kotlin.concurrent.thread

fun Node.bfsSequential(destination: Node): Long {
    this.distance = 0
    val q = Queue<Node>()
    q.add(this, -1)
    while (!q.empty) {
        val cur = q.poll()
        for (e in cur.outgoingEdges) {
            if (e.to.distance == Long.MAX_VALUE) {
                e.to.distance = cur.distance + 1
                q.add(e.to, -1)
            }
        }
    }
    return destination.distance
}

fun Node.bfsParallel(destination: Node, parallelism: Int = 0): Long {
    val workers = if (parallelism != 0) parallelism else Runtime.getRuntime().availableProcessors()
    // The distance to the start node is `0`
    this.distance = 0
    // Create a priority (by distance) queue and add the start node into it
    val q = MultiQueue<Node>(workers)
    q.add(this, 0)
    // Run worker threads and wait until the total work is done
    val onFinish = Phaser(workers + 1) // `arrive()` should be invoked at the end by each worker
    val waiters = AtomicInteger(0)
    repeat(workers) {
        thread {
            worker_step@while (true) {
                var incWaiters = false
                var cur: Node?
                poll@while (true) {
                    cur = q.poll()
                    if (cur != null) break@poll
                    if (!incWaiters) {
                        incWaiters = true
                        waiters.incrementAndGet()
                    }
                    if (waiters.get() == workers) break@worker_step
                }
                if (incWaiters) waiters.getAndDecrement()
                // Relax the edges
                val nodeDistance = cur!!.distance
                if (!cur.updateLastDistance(nodeDistance)) continue
                cur.incChanges()
                for (e in cur.outgoingEdges) {
                    val newDistance = nodeDistance + 1
                    update_distance@ while (true) {
                        val toDistance = e.to.distance
                        if (toDistance <= newDistance) break@update_distance
                        if (e.to.casDistance(toDistance, newDistance)) {
                            q.add(e.to, newDistance)
                            break@update_distance
                        }
                    }
                }
            }
            onFinish.arrive()
        }
    }
    onFinish.arriveAndAwaitAdvance()
    // Return the result
    return destination.distance
}

class MultiQueue<T: PQElement<T>>(parallelism: Int) {
    private val queues = Array<Queue<T>>(parallelism * 2) { Queue() }
    private val totalQueues get() = queues.size

    // Returns null if the queue is empty
    fun poll(): T? {
        while (true) {
            if (nonEmptyHeaps == 0) return null
            var i1: Int
            var i2: Int
            while (true) {
                i1 = ThreadLocalRandom.current().nextInt(totalQueues)
                i2 = ThreadLocalRandom.current().nextInt(totalQueues)
                if (i1 == i2) continue
                if (i1 > i2) { val t = i1; i1 = i2; i2 = t }
                break
            }
            val h1 = queues[i1]
            val h2 = queues[i2]
            synchronized(h1) { synchronized(h2) {
                if (!(h1.empty && h2.empty)) {
                    if (h1.empty) return pollInternal(h2)
                    if (h2.empty) return pollInternal(h1)
                    // Choose the smallest one
                    val t1 = h1.firstTime()
                    val t2 = h2.firstTime()
                    return if (t1 < t2) pollInternal(h1) else pollInternal(h2)
                }
            }}
        }
    }

    private fun pollInternal(q: Queue<T>): T {
        val x = q.poll()
        if (q.empty) decNonEmptyHeaps()
        return x
    }

    fun add(x: T, time: Long) {
        val q = queues[ThreadLocalRandom.current().nextInt(totalQueues)]
        synchronized(q) {
            val wasEmpty = q.empty
            q.add(x, time)
            if (wasEmpty) incNonEmptyHeaps()
        }
    }

    private fun incNonEmptyHeaps() = NON_EMPTY_HEAPS_UPDATER.getAndAdd(this, 1)
    private fun decNonEmptyHeaps() = NON_EMPTY_HEAPS_UPDATER.getAndAdd(this, -1)

    private companion object {
        val NON_EMPTY_HEAPS_UPDATER = AtomicIntegerFieldUpdater.newUpdater(MultiQueue::class.java, "nonEmptyHeaps")
    }

    @Volatile
    private var nonEmptyHeaps: Int = 0
}

private class Queue<T> {
    private var data = arrayOfNulls<Any>(128)
    private var times = LongArray(128)
    private var head = 0
    private var tail = 0

    val empty get() = head == tail

    fun add(x: T, time: Long) {
        data[tail] = x
        times[tail] = time
        tail++
        if (tail == data.size) tail = 0
        if (tail == head) grow()
    }

    fun firstTime(): Long {
        check(!empty)
        return times[head]
    }

    fun poll(): T {
        check(!empty)
        val x = data[head]
        data[head] = null
        head++
        if (head == data.size) head = 0
        return x as T
    }

    private fun grow() {
        val p = head
        val n = data.size
        val r = n - p // number of elements to the right of p
        val newCapacity = data.size * 2

        val a = arrayOfNulls<Any>(newCapacity)
        System.arraycopy(data, p, a, 0, r)
        System.arraycopy(data, 0, a, r, p)
        data = a

        val t = LongArray(newCapacity)
        System.arraycopy(times, p, t, 0, r)
        System.arraycopy(times, 0, t, r, p)
        times = t

        head = 0
        tail = n
    }
}