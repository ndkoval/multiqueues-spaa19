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

        assertEquals(2, a.dijkstraSequential(b))
        assertEquals(2, a.dijkstraParallel(b))

        assertEquals(4, a.dijkstraSequential(c))
        assertEquals(4, a.dijkstraParallel(c))

        assertEquals(1, a.dijkstraSequential(d))
        assertEquals(1, a.dijkstraParallel(d))

        assertEquals(5, a.dijkstraSequential(e))
        assertEquals(5, a.dijkstraParallel(e))
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

        assertEquals(1, a.bfsSequential(b))
        assertEquals(1, a.bfsParallel(b))

        assertEquals(2, d.bfsSequential(e))
        assertEquals(2, d.bfsParallel(e))

        assertEquals(2, a.bfsSequential(e))
        assertEquals(2, a.bfsParallel(e))

        assertEquals(1, b.bfsSequential(e))
        assertEquals(1, b.bfsParallel(e))
    }
}