package pagerank

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession

object CheckConvergenceJob extends SparkJob {

  override val name: String = "check-convergence"

  override def run(config: Config, spark: SparkSession): Unit = {

    import spark.implicits._
    def read(path: String): VertexRDD = spark.read.parquet(path).as[Vertex].rdd

    val oldVersion = read(config.getString(s"$name.old-version"))
    val newVersion = read(config.getString(s"$name.new-version"))

    logger.error(f"Sum of ranks: old = ${oldVersion.map(_._2).sum()}, new = ${newVersion.map(_._2).sum()}")
    logger.error(f"Sum of component-wise differences: ${computeDelta(oldVersion, newVersion)}%.15f")
  }

  private def computeDelta(left: VertexRDD, right: VertexRDD): Value =
    left.join(right).map { case (_, (l, r)) => math.abs(l - r) }.sum()
}
