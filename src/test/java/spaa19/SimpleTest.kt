package spaa19

import org.junit.Test
import kotlin.test.assertEquals

class SimpleTest {

    @Test
    fun `small graph 1`() {
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

        assertEquals(2, a.shortestPathSequential(b))
        assertEquals(2, a.shortestPathParallel(b))

        assertEquals(4, a.shortestPathSequential(c))
        assertEquals(4, a.shortestPathParallel(c))

        assertEquals(1, a.shortestPathSequential(d))
        assertEquals(1, a.shortestPathParallel(d))

        assertEquals(5, a.shortestPathSequential(e))
        assertEquals(5, a.shortestPathParallel(e))
    }
}