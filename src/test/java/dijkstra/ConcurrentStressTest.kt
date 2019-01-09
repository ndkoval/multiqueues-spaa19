package dijkstra

import org.junit.Test
import java.util.*
import kotlin.test.assertEquals

class ConcurrentStressTest {

    @Test
    fun `test on trees`() {
        testOnRandomGraphs(100, 99)
    }

    @Test
    fun `test on very small graphs`() {
        testOnRandomGraphs(6, 13)
    }

    @Test
    fun `test on small graphs`() {
        testOnRandomGraphs(100, 1000)
    }

    @Test
    fun `test on big graphs`() {
        testOnRandomGraphs(1000, 10000)
    }

    private fun testOnRandomGraphs(nodes: Int, edges: Int) {
        val r = Random()
        repeat(GRAPHS) {
            val nodesList = randomConnectedGraph(nodes, edges)
            repeat(SEARCHES) {
                val from = nodesList[r.nextInt(nodes)]
                val to = nodesList[r.nextInt(nodes)]
                assertEquals(from.shortestPathSequential(to), from.shortestPathParallel(to))
            }
        }
    }

}

private const val GRAPHS = 100
private const val SEARCHES = 100
