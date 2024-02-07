package pagerank

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ListBuffer

final case class PageRankGraph
(numVertices: Id,
 edges: EdgeRDD,
 vertices: RichVertexRDD) {

  private val logger = Logger(getClass)

  require(numVertices >= 2, "Number of vertices must be greater than or equal to 2")

  def updateVertexValues(newVertices: VertexRDD): PageRankGraph = {
    val updatedVertices = vertices
      .leftOuterJoin(newVertices)
      .map { case (id, (meta, newValueOpt)) =>
        (id, newValueOpt.map(newValue => meta.copy(_1 = newValue)).getOrElse(meta))
      }

    val totalDelta = 1.0 - updatedVertices.map(_._2._1).sum()

    val normalizedVertices = if (math.abs(totalDelta) > EPS) {
      val perNodeDelta = totalDelta / numVertices
      logger.error(s"ranks are not normalized: delta = $totalDelta, perNodeDelta = $perNodeDelta")
      updatedVertices.map { case (id, meta) => (id, meta.copy(_1 = meta._1 + perNodeDelta)) }
    } else {
      updatedVertices
    }

    val newGraph = PageRankGraph(numVertices, edges, normalizedVertices.persist(vertices.getStorageLevel))
    vertices.unpersist()
    newGraph
  }

  def printInfo(): Unit = {
    val messages = ListBuffer.empty[String]
    messages.append(s"Number of vertices = $numVertices")

    val numSelfReferences = edges.filter(e => e._1 == e._2).map(_._1).distinct().count()
    messages.append(s"Number of vertices with self-referencing edges = $numSelfReferences")

    val numDanglingVertices = vertices.filter(_._2._3).count()
    messages.append(s"Number of dangling vertices = $numDanglingVertices")

    val totalRanks = vertices.map(_._2._1).sum()
    messages.append(s"Sum of ranks = $totalRanks")

    val totalDelta = math.abs(1.0 - totalRanks)
    messages.append(s"Delta = $totalDelta")

    val perNodeDelta = totalDelta / numVertices
    messages.append(s"Per node delta = $perNodeDelta")

    logger.error("graph info:\n\t" + messages.mkString("\n\t"))
  }
}

object PageRankGraph {

  def save(spark: SparkSession, graph: PageRankGraph, path: String): Unit = {
    import spark.implicits._
    Metadata.save(spark, Metadata(graph.numVertices), s"$path/stats")
    graph.edges.toDS().write.parquet(s"$path/edges")
    graph.vertices.toDS().write.parquet(s"$path/vertices")
  }

  def load
  (spark: SparkSession,
   path: String,
   edgesStorageLevel: StorageLevel,
   verticesStorageLevel: StorageLevel): PageRankGraph = {

    import spark.implicits._

    PageRankGraph(
      Metadata.load(spark, s"$path/stats").numVertices,
      edges = spark.read.parquet(s"$path/edges").as[Edge].rdd.persist(edgesStorageLevel),
      vertices = spark.read.parquet(s"$path/vertices").as[RichVertex].rdd.persist(verticesStorageLevel)
    )
  }

  def fromEdgesWithUniformPriors
  (edges: EdgeRDD,
   tmpStorageLevel: StorageLevel,
   edgesStorageLevel: StorageLevel,
   verticesStorageLevel: StorageLevel): PageRankGraph = {

    val linkCounts = {
      val inLinks = edges.map(e => (e._2, 1L)).reduceByKey(_ + _)
      val outLinks = edges.map(e => (e._1, 1L)).reduceByKey(_ + _)
      inLinks.fullOuterJoin(outLinks).persist(tmpStorageLevel)
    }

    val numVertices = linkCounts.count()

    val vertices = {
      val prior = 1.0 / numVertices
      linkCounts.map { case (id, (inOpt, outOpt)) =>
        val in = inOpt.getOrElse(0L)
        val out = outOpt.getOrElse(0L)
        val isDangling = in > 0L && out == 0L
        (id, (prior, out, isDangling))
      }
    }

    linkCounts.unpersist()

    PageRankGraph(
      numVertices,
      edges.persist(edgesStorageLevel),
      vertices.persist(verticesStorageLevel)
    )
  }
}
