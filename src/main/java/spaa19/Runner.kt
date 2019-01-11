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
import kotlin.collections.ArrayList

private const val WARMUP_ITERATIONS = 3
private const val ITERATIONS = 10
private val THREADS = arrayOf(1, 2, 4, 8, 16, 32, 64, 96, 128, 144)
private val GRAPH_FILES = listOf(
        Triple("RAND-1M-10M", "rand", "1000000 10000000"), // 1M nodes and 10M edges
//        Triple("CTR-DISTANCE", "gr gz", "http://www.dis.uniroma1.it/challenge9/data/USA-road-d/USA-road-d.CTR.gr.gz"),
//        Triple("CTR-TRAVEL-TIME", "gr gz", "http://www.dis.uniroma1.it/challenge9/data/USA-road-t/USA-road-t.CTR.gr.gz"),
        Triple("USA-DISTANCE", "gr gz", "http://www.dis.uniroma1.it/challenge9/data/USA-road-d/USA-road-d.USA.gr.gz"),
//        Triple("USA-TRAVEL-TIME", "gr gz", "http://www.dis.uniroma1.it/challenge9/data/USA-road-t/USA-road-t.USA.gr.gz")
        Triple("LIVE-JOURNAL", "txt gz", "https://snap.stanford.edu/data/soc-LiveJournal1.txt.gz")
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
        for ((graphName, graphType, graphUrl) in GRAPH_FILES) {
            println("=== $graphName ===")
            val graphNodes = downloadOrCreateAndParseGraph(graphName, graphType, graphUrl)

//            saveAsDot("$graphName.dot", graphNodes)

            val from = graphNodes[0]
            val to = graphNodes[rand.nextInt(graphNodes.size - 1) + 1]
            val startTime = System.currentTimeMillis()
            val validResult = algo.sequential(from, to)
            println("SEQ: ${System.currentTimeMillis() - startTime}ms")
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

fun saveAsDot(filename: String, graph: List<Node>) {
    PrintWriter(filename).use { pw ->
        pw.println("digraph graphname {")
        graph.forEach { from ->
            from.outgoingEdges.forEach { e ->
                val to = e.to
                pw.println("\t${from.hashCode()} -> ${to.hashCode()};")
            }
        }
        pw.println("}")
    }
}

fun downloadOrCreateAndParseGraph(name: String, type: String, url: String): List<Node> {
    val gz = type.endsWith("gz")
    val ext = type.split(" ")[0]
    val graphFile = "$name." + (if (ext == "rand") "gr" else ext) + (if (gz) ".gz" else "")
    if (!Paths.get(graphFile).toFile().exists()) {
        if (ext == "rand") {
            val parts = url.split(" ")
            val n = parts[0].toInt()
            val m = parts[1].toInt()
            println("Generating $graphFile as a random graph with $n nodes and $m edges")
            val graphNodes = randomConnectedGraph(n, m)
            writeGrFile(graphFile, graphNodes)
            println("Generated $graphFile")
        } else {
            println("Downloading $graphFile from $url")
            val input = Channels.newChannel(URL(url).openStream())
            val output = FileOutputStream(graphFile)
            output.channel.transferFrom(input, 0, Long.MAX_VALUE)
            println("Downloaded $graphFile")
        }
    }
    return when {
        ext == "rand" || ext == "gr" -> parseGrFile(graphFile, gz)
        ext == "txt" -> parseTxtFile(graphFile, gz)
        else -> error("Unknown graph type: $ext")
    }
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

fun parseGrFile(filename: String, gziped: Boolean): List<Node> {
    val nodes = mutableListOf<Node>()
    val inputStream = if (gziped) GZIPInputStream(FileInputStream(filename)) else FileInputStream(filename)
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

fun parseTxtFile(filename: String, gziped: Boolean): List<Node> {
    val rand = Random(0)
    val nodes = ArrayList<Node>()
    val inputStream = if (gziped) GZIPInputStream(FileInputStream(filename)) else FileInputStream(filename)
    InputStreamReader(inputStream).buffered().useLines { it.forEach { line ->
        when {
            line.startsWith("# ") -> {} // just ignore
            else -> {
                val parts = line.split(" ", "\t")
                val from = parts[0].toInt()
                val to   = parts[1].toInt()
                val w    = rand.nextInt(100)
                while (nodes.size <= from || nodes.size <= to) nodes.add(Node())
                nodes[from].addEdge(Edge(nodes[to], w))
            }
        }
    }
    }
    return nodes
}

// Returns the total time in nanoseconds
fun run(graph: List<Node>, results: MutableList<Result>?, block: () -> Unit) {
    graph.parallelStream().forEach { node ->
        node.distance = Long.MAX_VALUE
        node.lastDistance = Long.MAX_VALUE
        node.changes = 0
    }
    val startTime = System.nanoTime()
    block()
    val totalTime = System.nanoTime() - startTime
    val processedNodes = graph.fold(0) { cur, node -> cur + node.changes }
    if (results != null) results += Result(totalTime, processedNodes.toDouble() / graph.size)
}