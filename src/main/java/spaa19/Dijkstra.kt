package spaa19

import org.nield.kotlinstatistics.standardDeviation
import java.io.*
import java.net.URL
import java.nio.channels.Channels
import java.nio.file.Paths
import java.util.*
import java.util.concurrent.Phaser
import java.util.concurrent.atomic.AtomicInteger
import java.util.zip.GZIPInputStream
import kotlin.Comparator
import kotlin.concurrent.thread

// SEQUENTIAL VERSION

val NODE_DISTANCE_COMPARATOR = Comparator<Node> { o1, o2 -> o1!!.distance.compareTo(o2!!.distance) }

// Returns `Integer.MAX_VALUE` is a path has not been found.
fun Node.dijkstraSequential(destination: Node): Long {
    this.distance = 0
    val q = java.util.PriorityQueue<Node>(NODE_DISTANCE_COMPARATOR)
    q.add(this)
    while (q.isNotEmpty()) {
        val cur = q.poll()
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
fun Node.dijkstraParallel(destination: Node, parallelism: Int = 0): Long {
    val workers = if (parallelism != 0) parallelism else Runtime.getRuntime().availableProcessors()
    // The distance to the start node is `0`
    this.distance = 0
    // Create a priority (by distance) queue and add the start node into it
    val q = MultiPriorityQueue<Node>(workers)
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