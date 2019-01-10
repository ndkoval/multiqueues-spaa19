package spaa19

import org.nield.kotlinstatistics.standardDeviation
import java.io.*
import java.net.URL
import java.nio.channels.Channels
import java.nio.file.Paths
import java.util.concurrent.Phaser
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater
import java.util.zip.GZIPInputStream
import kotlin.Comparator
import kotlin.concurrent.thread

// RUNNER

private const val WARMUP_ITERATIONS = 3
private const val ITERATIONS = 10
private val THREADS = arrayOf(1, 2, 4, 8, 16, 32, 64, 128, 144)
private val GRAPH_FILES = mapOf(
        Pair("RAND-1M-100M", "rand 1000000 100000000"), // 1M nodes and 100M edges
        Pair("CTR-DISTANCE", "http://www.dis.uniroma1.it/challenge9/data/USA-road-d/USA-road-d.CTR.gr.gz"),
        Pair("CTR-TRAVEL-TIME", "http://www.dis.uniroma1.it/challenge9/data/USA-road-t/USA-road-t.CTR.gr.gz"),
        Pair("USA-DISTANCE", "http://www.dis.uniroma1.it/challenge9/data/USA-road-d/USA-road-d.USA.gr.gz"),
        Pair("USA-TRAVEL-TIME", "http://www.dis.uniroma1.it/challenge9/data/USA-road-t/USA-road-t.USA.gr.gz")
)

fun main() {
    for ((graphName, graphUrl) in GRAPH_FILES) {
        println("=== $graphName ===")
        val graphNodes = downloadOrCreateAndParseGrFile(graphName, graphUrl)
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
}

fun Double.format(digits: Int) = java.lang.String.format("%.${digits}f", this)

data class Result(val time: Long, val overhead: Double)

fun downloadOrCreateAndParseGrFile(graphName: String, graphUrl: String): List<Node> {
    val gz = graphUrl.endsWith(".gz")
    val graphFile = "$graphName.gr" + (if (gz) ".gz" else "")
    if (!Paths.get(graphFile).toFile().exists()) {
        if (graphUrl.startsWith("rand ")) {
            val parts = graphUrl.split(" ")
            val n = parts[1].toInt()
            val m = parts[2].toInt()
            println("Generating $graphFile as a random graph with $n nodes and $m edges")
            val graphNodes = randomConnectedGraph(n, m)
            writeGrFile(graphFile, graphNodes)
            println("Generated $graphFile")
        } else {
            println("Downloading $graphFile from $graphUrl")
            val input = Channels.newChannel(URL(graphUrl).openStream())
            val output = FileOutputStream(graphFile)
            output.channel.transferFrom(input, 0, Long.MAX_VALUE)
            println("Downloaded $graphFile")
        }
    }
    return parseGrFile(graphFile)
}

fun writeGrFile(filename: String, graphNodes: List<Node>) {
    var curId = 1
    val nodeIds = mutableMapOf<Node, Int>()
    var m = 0
    graphNodes.forEach { node ->
        nodeIds[node] = curId++
        m += node.outgoingEdges.size
    }
    PrintWriter(filename).use { pw ->
        pw.println("p sp ${graphNodes.size} $m")
        graphNodes.forEach { from ->
            from.outgoingEdges.forEach { e ->
                pw.println("a ${nodeIds[from]} ${nodeIds[e.to]} ${e.weight}")
            }
        }
    }
}

fun parseGrFile(filename: String): List<Node> {
    val nodes = mutableListOf<Node>()
    val inputStream = if (filename.endsWith(".gz")) GZIPInputStream(FileInputStream(filename)) else FileInputStream(filename)
    InputStreamReader(inputStream).buffered().useLines { it.forEach { line ->
        when {
            line.startsWith("c ") -> {} // just ignore
            line.startsWith("p sp ") -> {
                val n = line.split(" ")[2].toInt()
                repeat(n) { nodes.add(Node()) }
            }
            line.startsWith("a ") -> {
                val parts = line.split(" ")
                val from = nodes[parts[1].toInt() - 1]
                val to = nodes[parts[2].toInt() - 1]
                val w = parts[3].toInt()
                from.addEdge(Edge(to, w))
            }
        }
        }
    }
    return nodes
}

// Returns the total time in nanoseconds
fun run(graph: List<Node>, results: MutableList<Result>?, block: () -> Unit) {
    val startTime = System.nanoTime()
    block()
    val totalTime = System.nanoTime() - startTime
    val processedNodes = graph.fold(0) { cur, node -> cur + node.changes }
    graph.forEach { node ->
        node.distance = Long.MAX_VALUE
        node.changes = 0
    }
    if (results != null) results += Result(totalTime, processedNodes.toDouble() / graph.size)
}

// SEQUENTIAL VERSION

val NODE_DISTANCE_COMPARATOR = Comparator<Node> { o1, o2 -> o1!!.distance.compareTo(o2!!.distance) }

// Returns `Integer.MAX_VALUE` is a path has not been found.
fun Node.shortestPathSequential(destination: Node): Long {
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
fun Node.shortestPathParallel(destination: Node, parallelism: Int = 0): Long {
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