package dijkstra

import org.nield.kotlinstatistics.standardDeviation
import java.util.*
import java.util.concurrent.Phaser
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater
import kotlin.Comparator
import kotlin.concurrent.thread

// RUNNER

// Graphs: http://iss.ices.utexas.edu/projects/galois/downloads/lonestar-cpu-inputs.tar.xz

private const val WARMUP_ITERATIONS = 1
private const val ITERATIONS = 10
private const val NODES = 100000
private const val EDGES = NODES * 10
private val THREADS = arrayOf(1, 2, 4, 8, 16, 32, 64, 128, 144)

fun main() {
    val graphNodes = randomConnectedGraph(NODES, EDGES)
    val from = graphNodes.first()
    val to = graphNodes.last()
    val validResult = from.shortestPathSequential(to)

    for (t in THREADS) {
        // Warm-Up
        repeat(WARMUP_ITERATIONS) {
            run(graphNodes, null) { check(validResult == from.shortestPathParallel(to, parallelism = t)) }
        }
        // Run the optimized code!
        val results = mutableListOf<Result>()
        repeat(ITERATIONS) {
            run(graphNodes, results) { check(validResult == from.shortestPathParallel(to, parallelism = t)) }
        }
        println("THREADS=$t, " +
                "time_avg=${results.map { it.time.toDouble() / 1_000_000 }.average().format(0)}ms, " +
                "overhead_avg=${results.map { it.overhead }.average().format(4)}, " +
                "overhead_std=${results.map { it.overhead }.standardDeviation().format(4)}"
        )
    }
}

fun Double.format(digits: Int) = java.lang.String.format("%.${digits}f", this)

data class Result(val time: Long, val overhead: Double)

// Returns the total time in nanoseconds
fun run(graph: List<Node>, results: MutableList<Result>?, block: () -> Unit) {
    val startTime = System.nanoTime()
    block()
    val totalTime = System.nanoTime() - startTime
    val processedNodes = graph.fold(0) { cur, node -> cur + node.changes }
    graph.forEach { node ->
        node.distance = Long.MAX_VALUE
        node.lastDistance = Long.MAX_VALUE
        node.changes = 0
    }
    if (results != null) results += Result(totalTime, processedNodes.toDouble() / graph.size)
}

// SEQUENTIAL VERSION

val NODE_DISTANCE_COMPARATOR = Comparator<Node> { o1, o2 -> o1!!.distance.compareTo(o2!!.distance) }

// Returns `Integer.MAX_VALUE` is a path has not been found.
fun Node.shortestPathSequential(destination: Node): Long {
    this.distance = 0
    val q = PriorityQueue<Node>(NODE_DISTANCE_COMPARATOR)
    q.add(this)
    while (q.isNotEmpty()) {
        val cur = q.poll()
        if (cur.distance >= cur.lastDistance) continue
        cur.lastDistance = cur.distance
        cur.changes++
        for (e in cur.outgoingEdges) {
            if (e.to.distance > cur.distance + e.weight) {
                e.to.distance = cur.distance + e.weight
                q.add(e.to)
            }
        }
    }
    return destination.distance
}

// PARALLEL VERSION

// Returns `Integer.MAX_VALUE` is a path has not been found
fun Node.shortestPathParallel(destination: Node, parallelism: Int = 0): Long {
    val workers = if (parallelism != 0) parallelism else Runtime.getRuntime().availableProcessors()
    // The distance to the start node is `0`
    this.distance = 0
    // Create a priority (by distance) queue and add the start node into it
    val q = MultiPriorityQueue(workers)
    q.add(this)
    // Run worker threads and wait until the total work is done
    val onFinish = Phaser(workers + 1) // `arrive()` should be invoked at the end by each worker
    val waiters = AtomicInteger(0)
    repeat(workers) {
        thread {
            worker_step@while (true) {
                var incWaiters = false
                var node: Node?
                poll@while (true) {
                    node = q.poll()
                    if (node != null) break@poll
                    if (!incWaiters) {
                        incWaiters = true
                        waiters.incrementAndGet()
                    }
                    if (waiters.get() == workers) break@worker_step
                }
                if (incWaiters) waiters.getAndDecrement()
                // Relax the edges
                val nodeDistance = node!!.distance
                if (!node.updateLastDistance(nodeDistance)) continue
                node.incChanges()
                for (e in node.outgoingEdges) {
                    val newDistance = nodeDistance + e.weight
                    update_distance@ while (true) {
                        val toDistance = e.to.distance
                        if (toDistance <= newDistance) break@update_distance
                        if (e.to.casDistance(toDistance, newDistance)) {
                            q.add(e.to)
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

private class MultiPriorityQueue(parallelism: Int) {
    private val heaps = Array<PriorityQueue<NodeWithDistance>>(parallelism * 2) { PriorityQueue() }
    private val totalHeaps get() = heaps.size

    @Volatile
    private var nonEmptyHeaps: Int = 0

    // Returns null if the queue is empty
    fun poll(): Node? {
        while (true) {
            if (nonEmptyHeaps == 0) return null
            val i1 = ThreadLocalRandom.current().nextInt(totalHeaps - 1)
            val i2 = ThreadLocalRandom.current().nextInt(i1 + 1, totalHeaps)
            val h1 = heaps[i1]
            val h2 = heaps[i2]
            synchronized(h1) { synchronized(h2) {
                if (!(h1.isEmpty() && h2.isEmpty())) {
                    if (h1.isEmpty()) return pollInternal(h2)
                    if (h2.isEmpty()) return pollInternal(h1)
                    // Choose the smallest one
                    val e1 = h1.peek()
                    val e2 = h2.peek()
                    return if (e1.distance < e2.distance) pollInternal(h1) else pollInternal(h2)
                }
            }}
        }
    }

    private fun pollInternal(heap: PriorityQueue<NodeWithDistance>): Node? {
        val res = heap.poll()
        if (heap.isEmpty()) decNonEmptyHeaps()
        return res.node
    }

    // Adds the specified element to this queue
    fun add(element: Node) {
        val i = ThreadLocalRandom.current().nextInt(totalHeaps)
        val h = heaps[i]
        synchronized(h) {
            val wasEmpty = h.isEmpty()
            h.add(NodeWithDistance(element))
            if (wasEmpty) incNonEmptyHeaps()
        }
    }

    private fun incNonEmptyHeaps() = NON_EMPTY_HEAPS_UPDATER.getAndAdd(this, 1)
    private fun decNonEmptyHeaps() = NON_EMPTY_HEAPS_UPDATER.getAndAdd(this, -1)

    private companion object {
        val NON_EMPTY_HEAPS_UPDATER = AtomicIntegerFieldUpdater.newUpdater(MultiPriorityQueue::class.java, "nonEmptyHeaps")
    }

    private class NodeWithDistance(val node: Node) : Comparable<NodeWithDistance> {
        val distance = node.distance
        override fun compareTo(other: NodeWithDistance): Int {
            if (this.distance < other.distance) return -1
            if (this.distance == other.distance) return 0
            return 1
        }
    }
}