import org.apache.spark.ml.feature.BucketedRandomProjectionLSH
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object LSHClustering {

  def applyLSH(spark: SparkSession, patternsDF: DataFrame): DataFrame = {
    import spark.implicits._

    if (patternsDF.isEmpty) {
      println("No patterns to cluster.")
      return spark.emptyDataFrame
    }

    val lsh = new BucketedRandomProjectionLSH()
      .setBucketLength(0.2)
      .setNumHashTables(5)
      .setInputCol("features")
      .setOutputCol("hashes")

    val model = lsh.fit(patternsDF)
    val transformedDF = model.transform(patternsDF)

    val clusteredDF = transformedDF
      .groupBy($"hashes")
      .agg(
        collect_list($"label").as("labelsInCluster"),
        collect_list($"properties").as("propertiesInCluster")
      )

    clusteredDF
  }
}
