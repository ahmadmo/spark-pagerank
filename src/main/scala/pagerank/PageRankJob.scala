package pagerank

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import scala.util.Try

object PageRankJob extends SparkJob {

  override val name: String = "page-rank"

  override def run(config: Config, spark: SparkSession): Unit = {

    val input = config.getString(s"$name.input")
    val output = config.getString(s"$name.output")
    val dampingFactor = config.getDouble(s"$name.damping-factor")
    val printInfo = config.getBoolean(s"$name.print-info")
    val maxIterationsOpt = Try(config.getInt(s"$name.max-iterations")).toOption
    val convergenceThresholdOpt = Try(config.getDouble(s"$name.convergence-threshold")).toOption
    val priorsOpt = Try(config.getString(s"$name.priors")).toOption

    require(
      maxIterationsOpt.isDefined || convergenceThresholdOpt.isDefined,
      "Neither Max iterations and Convergence threshold are specified"
    )

    spark.sparkContext.setCheckpointDir(s"${output}__checkpoints")

    val inputGraph = PageRankGraph.load(
      spark,
      input,
      edgesStorageLevel = StorageLevel.MEMORY_AND_DISK,
      verticesStorageLevel = StorageLevel.MEMORY_AND_DISK
    )

    import spark.implicits._

    val graph = priorsOpt match {
      case None => inputGraph
      case Some(priors) => inputGraph.updateVertexValues(spark.read.parquet(priors).as[Vertex].rdd)
    }

    if (printInfo) {
      graph.printInfo()
    }

    PageRank.run(graph, dampingFactor, maxIterationsOpt, convergenceThresholdOpt).toDS().write.parquet(output)
  }
}
