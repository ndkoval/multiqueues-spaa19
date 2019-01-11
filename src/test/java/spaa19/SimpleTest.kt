package spaa19

import org.junit.Test
import kotlin.test.assertEquals

// TODO clean nodes
class SimpleTest {

    @Test
    fun `Dijkstra on a small graph`() {
        val a = Node()
        val b = Node()
        val c = Node()
        val d = Node()
        val e = Node()
        a.addEdge(Edge(b, 2))
        a.addEdge(Edge(d, 1))
        b.addEdge(Edge(c, 4))
        b.addEdge(Edge(e, 5))
        c.addEdge(Edge(e, 1))
        d.addEdge(Edge(c, 3))
        val nodes = listOf(a, b, c, d, e)

        assertEquals(2, a.dijkstraSequential(b))
        clearNodes(nodes)
        assertEquals(2, a.dijkstraParallel(b))
        clearNodes(nodes)

        assertEquals(4, a.dijkstraSequential(c))
        clearNodes(nodes)
        assertEquals(4, a.dijkstraParallel(c))
        clearNodes(nodes)

        assertEquals(1, a.dijkstraSequential(d))
        clearNodes(nodes)
        assertEquals(1, a.dijkstraParallel(d))
        clearNodes(nodes)

        assertEquals(5, a.dijkstraSequential(e))
        clearNodes(nodes)
        assertEquals(5, a.dijkstraParallel(e))
        clearNodes(nodes)
    }

    @Test
    fun `BFS on a small graph`() {
        val a = Node()
        val b = Node()
        val c = Node()
        val d = Node()
        val e = Node()
        a.addEdge(Edge(b, 2))
        a.addEdge(Edge(d, 1))
        b.addEdge(Edge(c, 4))
        b.addEdge(Edge(e, 5))
        c.addEdge(Edge(e, 1))
        d.addEdge(Edge(c, 3))
        val nodes = listOf(a, b, c, d, e)

        assertEquals(1, a.bfsSequential(b))
        clearNodes(nodes)
        assertEquals(1, a.bfsParallel(b))
        clearNodes(nodes)

        assertEquals(2, d.bfsSequential(e))
        clearNodes(nodes)
        assertEquals(2, d.bfsParallel(e))
        clearNodes(nodes)

        assertEquals(2, a.bfsSequential(e))
        clearNodes(nodes)
        assertEquals(2, a.bfsParallel(e))
        clearNodes(nodes)

        assertEquals(1, b.bfsSequential(e))
        clearNodes(nodes)
        assertEquals(1, b.bfsParallel(e))
        clearNodes(nodes)
    }

    fun clearNodes(nodes: List<Node>) {
        nodes.forEach { n ->
            n.changes = 0
            n.distance = Long.MAX_VALUE
            n.lastDistance = Long.MAX_VALUE
        }
    }
}