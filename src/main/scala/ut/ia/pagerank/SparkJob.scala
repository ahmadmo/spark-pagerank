package ut.ia.pagerank

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.SparkSession

trait SparkJob {

  protected val logger = Logger(getClass)

  val name: String

  def run(config: Config, spark: SparkSession): Unit

  def main(args: Array[String]): Unit = {

    val config = args.headOption
      .map { configPath =>
        val file = new File(configPath)
        logger.info(s"loading config: $file")
        ConfigFactory.parseFile(file).resolve()
      }
      .getOrElse {
        logger.info("loading default config")
        ConfigFactory.load()
      }

    val spark = SparkSession
      .builder
      .appName(name)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    run(config, spark)
    spark.stop()
  }
}
