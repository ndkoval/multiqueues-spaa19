package spaa19

import org.nield.kotlinstatistics.standardDeviation
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.InputStreamReader
import java.io.PrintWriter
import java.net.URL
import java.nio.channels.Channels
import java.nio.file.Paths
import java.util.*
import java.util.zip.GZIPInputStream

private const val WARMUP_ITERATIONS = 3
private const val ITERATIONS = 10
private val THREADS = arrayOf(1, 2, 4, 8, 16, 32, 64, 96)
private val GRAPH_FILES = mapOf(
        Pair("RAND-1M-10M", "rand 1000000 10000000"), // 1M nodes and 10M edges
        Pair("CTR-DISTANCE", "http://www.dis.uniroma1.it/challenge9/data/USA-road-d/USA-road-d.CTR.gr.gz"),
        Pair("CTR-TRAVEL-TIME", "http://www.dis.uniroma1.it/challenge9/data/USA-road-t/USA-road-t.CTR.gr.gz"),
        Pair("USA-DISTANCE", "http://www.dis.uniroma1.it/challenge9/data/USA-road-d/USA-road-d.USA.gr.gz"),
        Pair("USA-TRAVEL-TIME", "http://www.dis.uniroma1.it/challenge9/data/USA-road-t/USA-road-t.USA.gr.gz")
)

private val SSSP_ALGOS = listOf(
        SSSPAlgo("BFS", Node::bfsSequential, Node::bfsParallel),
        SSSPAlgo("Dijkstra", Node::dijkstraSequential, Node::dijkstraParallel)
)
class SSSPAlgo(val name: String, val sequential: (Node, Node) -> Long, val parallel: (Node, Node, Int) -> Long)

fun main() {
    val rand = Random(0)
    for (algo in SSSP_ALGOS) {
        repeat(algo.name.length + 4) { print("#")}; println()
        println("# ${algo.name} #")
        repeat(algo.name.length + 4) { print("#")}; println()
        for ((graphName, graphUrl) in GRAPH_FILES) {
            println("=== $graphName ===")
            val graphNodes = downloadOrCreateAndParseGrFile(graphName, graphUrl)
            val from = graphNodes[rand.nextInt(graphNodes.size)]
            val to = graphNodes[rand.nextInt(graphNodes.size)]
            val validResult = algo.sequential(from, to)
            for (t in THREADS) {
                // Warm-Up
                repeat(WARMUP_ITERATIONS) {
                    run(graphNodes, null) { check(validResult == algo.parallel(from, to, t)) }
                }
                // Run the optimized code!
                val results = mutableListOf<Result>()
                repeat(ITERATIONS) {
                    run(graphNodes, results) { check(validResult == algo.parallel(from, to, t)) }
                }
                println("threads=$t, " +
                        "time_avg=${results.map { it.time.toDouble() / 1_000_000 }.average().format(0)}ms, " +
                        "time_std=${results.map { it.time.toDouble() / 1_000_000 }.standardDeviation().format(0)}ms, " +
                        "overhead_avg=${results.map { it.overhead }.average().format(4)}, " +
                        "overhead_std=${results.map { it.overhead }.standardDeviation().format(4)}"
                )
            }
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
        node.lastDistance = Long.MAX_VALUE
        node.changes = 0
    }
    if (results != null) results += Result(totalTime, processedNodes.toDouble() / graph.size)
}