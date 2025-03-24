import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.linalg.Vector  // Explicitly import Vector

object KMeansClustering {
  def applyKMeansNodes(spark: SparkSession, patternsDF: DataFrame, k: Int = 5): DataFrame = {
    import spark.implicits._

    if (patternsDF.isEmpty) {
      println("No patterns to cluster.")
      return spark.emptyDataFrame
    }

    val kmeans = new KMeans()
      .setK(k)
      .setSeed(1L)
      .setFeaturesCol("features")
      .setPredictionCol("cluster")

    val model = kmeans.fit(patternsDF)
    val transformedDF = model.transform(patternsDF)
    val clusteredDF = transformedDF
      .groupBy($"cluster")
      .agg(
        collect_list($"label").as("labelsInCluster"),
        collect_list($"properties").as("propertiesInCluster"),
        flatten(collect_list($"assignedNodeIds")).as("nodeIdsInCluster")
      )

    clusteredDF
  }

  def applyKMeansEdges(spark: SparkSession, df: DataFrame, k: Int = 20): DataFrame = {
    import spark.implicits._
    
    if (df.isEmpty) {
      println("No edge patterns to cluster.")
      return spark.emptyDataFrame
    }
    df.show()
    //remove rows with null values
    val df2 = df.na.drop()
    val kmeans = new KMeans()
      .setK(k)
      .setSeed(1L)
      .setFeaturesCol("features")
      .setPredictionCol("cluster")
      .setMaxIter(20)

    try {
      val model = kmeans.fit(df2)
      val transformedDF = model.transform(df2)

      val groupedDF = transformedDF
        .groupBy($"cluster")
        .agg(
          collect_list($"relationshipType").as("relsInCluster"),
          collect_list($"srcLabels").as("srcLabelsInCluster"),
          collect_list($"dstLabels").as("dstLabelsInCluster"),
          collect_list($"properties").as("propsInCluster"),
          flatten(collect_list($"assignedEdgeIds")).as("edgeIdsInCluster")
        )

      groupedDF
    } catch {
      case e: Exception =>
        println(s"K-means clustering failed: ${e.getMessage}")
        spark.emptyDataFrame
    }
  }
}