package pagerank

import com.typesafe.scalalogging.Logger
import org.apache.spark.storage.StorageLevel

object PageRank {

  private val logger = Logger(getClass)

  def run
  (graph: PageRankGraph,
   dampingFactor: Double,
   maxIterationsOpt: Option[Int],
   convergenceThresholdOpt: Option[Double]): VertexRDD = {

    require(graph.edges.getStorageLevel != StorageLevel.NONE, "Storage level of edges cannot be `NONE`")
    require(graph.vertices.getStorageLevel != StorageLevel.NONE, "Storage level of vertices cannot be `NONE`")

    require(dampingFactor >= 0.0 && dampingFactor < 1.0, "Damping factor must be in range [0, 1)")

    maxIterationsOpt.foreach(i => require(i >= 1, "Max iterations must be greater than or equal to 1"))
    convergenceThresholdOpt.foreach(t => require(t > 0.0 && t < 1.0, "Convergence threshold must be in range (0, 1)"))

    val maxIterations = maxIterationsOpt.getOrElse(Int.MaxValue)
    var newVertices = graph.vertices
    var converged = false

    for (numIterations <- 0 until maxIterations if !converged) {

      val oldVertices = newVertices

      newVertices = iterate(graph.edges, oldVertices, dampingFactor, graph.numVertices)
        .persist(oldVertices.getStorageLevel)

      val totalRanks = newVertices.map(_._2._1).sum()

      convergenceThresholdOpt match {
        case Some(t) =>
          val delta = computeDelta(oldVertices, newVertices)
          logger.error(s"numIterations = $numIterations, delta = $delta, totalRanks = $totalRanks")
          if (delta < t) {
            converged = true
            logger.error(s"converged")
          }
        case _ =>
          logger.error(s"numIterations = $numIterations, totalRanks = $totalRanks")
      }

      newVertices.checkpoint()
      oldVertices.unpersist(blocking = false)
    }

    newVertices.map { case (id, meta) => (id, meta._1) }
  }

  private def iterate
  (edges: EdgeRDD,
   vertices: RichVertexRDD,
   dampingFactor: Double,
   numVertices: Long): RichVertexRDD = {

    def calculateVertexUpdate(incomingSum: Value): Value =
      (dampingFactor * incomingSum) + ((1.0 - dampingFactor) / numVertices)

    def calculateDanglingVertexUpdate(value: Value): Value =
      dampingFactor * value / (numVertices - 1)

    def updateVertex
    (meta: VertexMetadata,
     incomingSumOpt: Option[Value],
     perVertexMissingMass: Value): VertexMetadata = {

      val incomingSum = incomingSumOpt.getOrElse(0.0)
      val newValue = calculateVertexUpdate(incomingSum) + perVertexMissingMass

      meta.copy(_1 = if (meta._3) newValue - calculateDanglingVertexUpdate(meta._1) else newValue)
    }

    val danglingMass = vertices.filter(_._2._3).map(_._2._1).sum()

    val incomingSumPerVertex = edges
      .join(vertices)
      .map { case (_, (dstId, (value, links, _))) => (dstId, value / links) }
      .reduceByKey(_ + _)

    val perVertexMissingMass = calculateDanglingVertexUpdate(danglingMass)

    vertices
      .leftOuterJoin(incomingSumPerVertex)
      .map { case (id, (meta, incomingSumOpt)) =>
        (id, updateVertex(meta, incomingSumOpt, perVertexMissingMass))
      }
  }

  private def computeDelta(left: RichVertexRDD, right: RichVertexRDD): Value =
    left.join(right).map { case (_, (l, r)) => math.abs(l._1 - r._1) }.sum()
}
