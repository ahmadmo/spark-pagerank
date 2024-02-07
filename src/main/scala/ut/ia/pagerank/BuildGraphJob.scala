package ut.ia.pagerank

import com.typesafe.config.Config
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object BuildGraphJob extends SparkJob {

  override val name: String = "build-graph"

  override def run(config: Config, spark: SparkSession): Unit = {

    val input = config.getString(s"$name.input")
    val output = config.getString(s"$name.output")
    val printInfo = config.getBoolean(s"$name.print-info")
    val numPartitions = config.getInt(s"$name.num-partitions")

    val inputRdd = spark.sparkContext.textFile(input, numPartitions).map { row =>
      val Array(srcId, dstId) = row.split(" ")
      (srcId.toLong, dstId.toLong)
    }

    val graph = PageRankGraph.fromEdgesWithUniformPriors(
      inputRdd,
      tmpStorageLevel = StorageLevel.MEMORY_ONLY,
      edgesStorageLevel = StorageLevel.MEMORY_AND_DISK,
      verticesStorageLevel = StorageLevel.MEMORY_AND_DISK
    )

    PageRankGraph.save(spark, graph, output)

    if (printInfo) {
      graph.printInfo()
    }
  }

}
