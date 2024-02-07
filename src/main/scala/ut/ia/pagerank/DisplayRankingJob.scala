package ut.ia.pagerank

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession

object DisplayRankingJob extends SparkJob {

  override val name: String = "display-ranking"

  override def run(config: Config, spark: SparkSession): Unit = {

    val vertexNamesInput = config.getString(s"$name.vertex-names-input")
    val graphInput = config.getString(s"$name.graph-input")
    val output = config.getString(s"$name.output")
    val numPartitions = config.getInt(s"$name.num-partitions")

    val inputRdd = spark.sparkContext.textFile(vertexNamesInput, numPartitions).map { row =>
      val parts = row.split(" ")
      (parts(0).toLong, parts.drop(1).mkString(" "))
    }

    import spark.implicits._
    val graph = spark.read.parquet(graphInput).as[Vertex].rdd

    inputRdd
      .join(graph)
      .sortBy(-_._2._2).zipWithIndex()
      .map { case ((id, (name, rank)), index) => (index, id, name, f"$rank%.15f") }
      .toDS().repartition(numPartitions).write.csv(output)
  }
}
