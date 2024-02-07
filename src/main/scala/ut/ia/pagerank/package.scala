package ut.ia

import org.apache.spark.rdd.RDD

package object pagerank {

  type Id = Long
  type Value = Double
  type Links = Long
  type Dangling = Boolean

  type Edge = (Id, Id)
  type Vertex = (Id, Value)
  type VertexMetadata = (Value, Links, Dangling)
  type RichVertex = (Id, VertexMetadata)

  type EdgeRDD = RDD[Edge]
  type VertexRDD = RDD[Vertex]
  type RichVertexRDD = RDD[RichVertex]

  val EPS: Double = 1.0E-15
}
