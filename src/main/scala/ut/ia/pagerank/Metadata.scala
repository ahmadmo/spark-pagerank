package ut.ia.pagerank

import org.apache.spark.sql.SparkSession

case class Metadata(numVertices: Long)

object Metadata {

  def save(spark: SparkSession, metadata: Metadata, path: String): Unit = {
    import spark.implicits._
    Seq(metadata).toDS().write.json(path)
  }

  def load(spark: SparkSession, path: String): Metadata = {
    import spark.implicits._
    spark.read.json(path).as[Metadata].head()
  }
}
