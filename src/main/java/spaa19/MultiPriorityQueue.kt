package spaa19

import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater

class MultiPriorityQueue<T: PQElement<T>>(parallelism: Int) {
    private val heaps = Array<PriorityQueue<T>>(parallelism * 2) { PriorityQueue() }
    private val totalHeaps get() = heaps.size

    // Returns null if the queue is empty
    fun poll(): T? {
        while (true) {
            if (nonEmptyHeaps == 0) return null
            var i1: Int
            var i2: Int
            while (true) {
                i1 = ThreadLocalRandom.current().nextInt(totalHeaps)
                i2 = ThreadLocalRandom.current().nextInt(totalHeaps)
                if (i1 != i2) {
                    if (i1 > i2) { val t = i1; i1 = i2; i2 = i1 }
                    break
                }
            }
            val h1 = heaps[i1]
            val h2 = heaps[i2]
            synchronized(h1) { synchronized(h2) {
                if (!(h1.empty && h2.empty)) {
                    if (h1.empty) {
                        val e2 = h2.peek()
                        if (pollInternal(h2, e2)) return e2
                    } else if (h2.empty) {
                        val e1 = h1.peek()
                        if (pollInternal(h1, e1)) return e1
                    } else {
                        // Choose the smallest one
                        val e1 = h1.peek()
                        val e2 = h2.peek()
                        if (e1 < e2) {
                            if (pollInternal(h1, e1)) return e1
                        } else {
                            if (pollInternal(h2, e2)) return e2
                        }
                    }
                }
            }}
        }
    }

    private fun pollInternal(heap: PriorityQueue<T>, x: T): Boolean {
        synchronized(x) {
            if (x.queue != heap) return false
            if (heap.peek() !== x) return false
            heap.poll()
            if (heap.empty) decNonEmptyHeaps()
            return true
        }
    }

    // Adds the specified element to this queue
    fun add(x: T) {
        while (true) {
            val heap = x.queue ?: heaps[ThreadLocalRandom.current().nextInt(totalHeaps)]
            synchronized(heap) { synchronized(x) {
                val xq = x.queue
                if (xq == null || xq == heap) { // check that the snapshot is correct, try again otherwise
                    val wasEmpty = heap.empty
                    heap.insert(x)
                    if (wasEmpty) incNonEmptyHeaps()
                    return
                }
            }}
        }
    }

    private fun incNonEmptyHeaps() = NON_EMPTY_HEAPS_UPDATER.getAndAdd(this, 1)
    private fun decNonEmptyHeaps() = NON_EMPTY_HEAPS_UPDATER.getAndAdd(this, -1)

    private companion object {
        val NON_EMPTY_HEAPS_UPDATER = AtomicIntegerFieldUpdater.newUpdater(MultiPriorityQueue::class.java, "nonEmptyHeaps")
    }

    @Volatile
    private var nonEmptyHeaps: Int = 0
}

class PriorityQueue<T: PQElement<T>> {
    private var queue = arrayOfNulls<Any>(11)
    private var size = 0

    fun insert(x: T) {
        when (x.queue) {
            this -> {
                siftUp(x.position)
                siftDown(x.position)
            }
            null -> {
                val i = size++
                if (i >= queue.size) grow()
                queue[i] = x
                x.queue = this
                x.position = i
                siftUp(i)
            }
            else -> error("Should be inserted to another queue")
        }
    }

    val empty get() = size == 0

    fun peek(): T {
        check(size > 0) { "Queue is empty" }
        return queue[0] as T
    }

    fun poll(): T {
        check(size > 0) { "Queue is empty" }
        val min = queue[0] as T
        val i = --size
        queue[0] = queue[i]
        (queue[0] as T).position = 0
        queue[i] = null
        if (i > 0) siftDown(0)
        min.queue = null
        return min
    }

    private fun swap(i: Int, j: Int) {
        val temp = queue[i]
        queue[i] = queue[j]
        queue[j] = temp
        (queue[i] as T).position = i
        (queue[j] as T).position = j
    }

    private fun siftUp(i: Int) {
        check(i <= size)
        if (size == 0) return
        var i = i
        var parent = (i - 1) / 2
        while ((queue[i] as T) < (queue[parent] as T)) {
            swap(i, parent)
            i = parent
            parent = (i - 1) / 2
        }
    }

    private fun siftDown(i: Int) {
        check(i <= size)
        if (size == 0) return
        var i = i
        while (2 * i + 1 < size) {
            val l = 2 * i + 1
            val r = 2 * i + 2
            var j = l
            if (r < size && (queue[r] as T) < (queue[l] as T)) j = r
            if ((queue[i] as T) <= (queue[j] as T)) break
            swap(i, j)
            i = j
        }
    }


    private fun grow() {
        val oldCapacity = queue.size
        // Double size if small; else grow by 50%
        val newCapacity = oldCapacity +
                if (oldCapacity < 64) oldCapacity + 2 else oldCapacity shr 1
        queue = queue.copyOf(newCapacity)
    }
}

interface PQElement<T: PQElement<T>>: Comparable<T> {
    var queue: PriorityQueue<T>?
    var position: Int
}