import org.apache.spark.ml.feature.BucketedRandomProjectionLSH
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object LSHClustering {

  def applyLSHNodes(spark: SparkSession, patternsDF: DataFrame): DataFrame = {
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
        collect_list($"properties").as("propertiesInCluster"),
        flatten(collect_list($"assignedNodeIds")).as("nodeIdsInCluster")
      )

    clusteredDF
  }

  def applyLSHEdges(spark: SparkSession, df: DataFrame): DataFrame = {
    import spark.implicits._

    if (df.isEmpty) {
      println("No edge patterns to cluster.")
      return spark.emptyDataFrame
    }

    val lsh = new BucketedRandomProjectionLSH()
      .setBucketLength(0.2)
      .setNumHashTables(10)
      .setInputCol("features")
      .setOutputCol("hashes")

    val model = lsh.fit(df)
    val transformedDF = model.transform(df)

    // Group by hashes and aggregate
    val groupedDF = transformedDF
      .groupBy($"hashes")
      .agg(
        collect_list($"relationshipType").as("relsInCluster"),
        collect_list($"srcLabels").as("srcLabelsInCluster"),
        collect_list($"dstLabels").as("dstLabelsInCluster"),
        collect_list($"properties").as("propsInCluster"),
        flatten(collect_list($"assignedEdgeIds")).as("edgeIdsInCluster")
      )

    groupedDF
  }
}
