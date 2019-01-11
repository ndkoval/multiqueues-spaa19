package spaa19

import java.util.*
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater
import java.util.concurrent.atomic.AtomicLongFieldUpdater
import kotlin.collections.ArrayList

class Node: PQElement<Node> {
    @Volatile
    override var queue: PriorityQueue<Node>? = null
    override var position: Int = -1

    override fun compareTo(other: Node): Int {
        while (true) {
            val curDistance = this.distance
            val otherDistance = other.distance
            if (curDistance == this.distance) {
                return when {
                    curDistance < otherDistance -> -1
                    curDistance == otherDistance -> 0
                    else -> 1
                }
            }
        }
    }

    private val _outgoingEdges = arrayListOf<Edge>()
    val outgoingEdges: List<Edge> = _outgoingEdges

    @Volatile var distance = Long.MAX_VALUE // USE ME FOR THE DIJKSTRA ALGORITHM!
    fun casDistance(from: Long, to: Long) = DISTANCE_UPDATER.compareAndSet(this, from, to)

    @Volatile var changes = 0
    fun incChanges() = CHANGES_UPDATER.getAndAdd(this, 1)

    fun addEdge(edge: Edge) {
        _outgoingEdges.add(edge)
    }

    @Volatile var lastDistance = Long.MAX_VALUE
    fun updateLastDistance(new: Long): Boolean {
        while(true) {
            val cur = lastDistance
            if (cur <= new) return false
            if (LAST_DISTANCE_UPDATER.compareAndSet(this, cur, new)) return true
        }
    }

    private companion object {
        val DISTANCE_UPDATER = AtomicLongFieldUpdater.newUpdater(Node::class.java, "distance")
        val LAST_DISTANCE_UPDATER = AtomicLongFieldUpdater.newUpdater(Node::class.java, "lastDistance")
        val CHANGES_UPDATER = AtomicIntegerFieldUpdater.newUpdater(Node::class.java, "changes")
    }
}

data class Edge(val to: Node, val weight: Int)

fun randomConnectedGraph(nodes: Int, edges: Int, maxWeight: Int = 100): List<Node> {
    require(edges >= nodes - 1)
    val r = Random(0) // ALWAYS USE THE SAME SEED
    val nodesList = List(nodes) { Node() }
    // generate a random connected graph with `nodes-1` edges
    val s = ArrayList(nodesList)
    var cur = s.removeAt(r.nextInt(s.size))
    val visited = mutableSetOf<Node>(cur)
    while (s.isNotEmpty()) {
        val neighbor = s.removeAt(r.nextInt(s.size))
        if (visited.add(neighbor)) {
            cur.addEdge(Edge(neighbor, r.nextInt(maxWeight)))
        }
        cur = neighbor
    }
    // add `edges - nodes + 1` random edges
    repeat(edges - nodes + 1) {
        while (true) {
            val first = nodesList[r.nextInt(nodes)]
            val second = nodesList[r.nextInt(nodes)]
            if (first == second) continue
            if (first.outgoingEdges.any { e -> e.to == second }) continue
            val weight = r.nextInt(maxWeight)
            first.addEdge(Edge(second, weight))
            second.addEdge(Edge(first, weight))
            break
        }
    }
    return nodesList
}