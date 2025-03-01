import org.apache.spark.ml.feature.BucketedRandomProjectionLSH
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object LSHClustering {
  private var existingClusters: Option[DataFrame] = None

  def applyLSH(spark: SparkSession, patternsDF: DataFrame): DataFrame = {
    import spark.implicits._

    if (patternsDF.isEmpty) {
      println("No new patterns to cluster.")
      return existingClusters.getOrElse(spark.emptyDataFrame)
    }

    val lsh = new BucketedRandomProjectionLSH()
      .setBucketLength(0.5)
      .setNumHashTables(5)
      .setInputCol("features")
      .setOutputCol("hashes")

    val model = lsh.fit(patternsDF)
    val transformedDF = model.transform(patternsDF)

    val allClusters = existingClusters match {
      case Some(prevClusters) => prevClusters.union(transformedDF)
      case None => transformedDF
    }

    existingClusters = Some(allClusters)

    val processedDF = allClusters.withColumn(
      "propertiesArray",
      when(col("properties").isNotNull, array(col("properties").cast("string")))
        .otherwise(array(lit("")))
    )

    val clusteredPatterns = processedDF
      .groupBy($"hashes")
      .agg(
        collect_set($"label").as("Labels"),
        collect_set($"propertiesArray").as("Patterns")
      )

    println("\n---- Merged Patterns ----")
    clusteredPatterns.collect().foreach { row =>
      val labels = row.getAs[Seq[String]]("Labels").mkString(", ")
      val patterns = row.getAs[Seq[Seq[String]]]("Patterns").flatMap(_.map(_.toString)).distinct.mkString(", ")
      println(s"Cluster [$labels]: $patterns")
    }

    clusteredPatterns
  }
}
