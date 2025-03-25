import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object KMeansClustering {

  def applyKMeansNodes(spark: SparkSession, patternsDF: DataFrame, k: Int = 10): DataFrame = {
    import spark.implicits._

    if (patternsDF.isEmpty) {
      println("No node patterns to cluster.")
      return spark.emptyDataFrame
    }

    val kmeans = new KMeans()
      .setK(k)
      .setSeed(1L)
      .setFeaturesCol("features")
      .setPredictionCol("cluster")

    val model = kmeans.fit(patternsDF)
    val clusteredDF = model.transform(patternsDF)

    val propCols = patternsDF.columns.filterNot(Seq("_nodeId", "_labels", "features",  "originalLabels").contains)
    val nonNullProps = array(
      propCols.map(c => when(col(c).isNotNull, lit(c)).otherwise(null)).toSeq: _*
    ).as("nonNullProps")

    clusteredDF
      .withColumn("nonNullProps", nonNullProps)
      .groupBy($"cluster")
      .agg(
        collect_set($"_labels").as("labelsInCluster"),
        collect_set(filter($"nonNullProps", x => x.isNotNull)).as("propertiesInCluster"),
        collect_list($"_nodeId").as("nodeIdsInCluster")
      )
  }

  def applyKMeansEdges(spark: SparkSession, df: DataFrame, k: Int = 10): DataFrame = {
    import spark.implicits._

    if (df.isEmpty) {
      println("No edge patterns to cluster.")
      return spark.emptyDataFrame
    }

    val kmeans = new KMeans()
      .setK(k)
      .setSeed(1L)
      .setFeaturesCol("features")
      .setPredictionCol("cluster")

    val model = kmeans.fit(df)
    val clusteredDF = model.transform(df)

    val propCols = df.columns.filter(_.startsWith("prop_"))

    val nonNullProps = array(
      propCols.map(c => when(col(c).isNotNull, lit(c)).otherwise(null)).toSeq: _*
    ).as("nonNullProps")

    clusteredDF
      .withColumn("nonNullProps", nonNullProps)
      .groupBy($"cluster")
      .agg(
        collect_set($"relationshipType").as("relsInCluster"),
        collect_set($"srcType").as("srcLabelsInCluster"),
        collect_set($"dstType").as("dstLabelsInCluster"),
        collect_set(filter($"nonNullProps", x => x.isNotNull)).as("propsInCluster"),
        collect_list(struct($"srcId", $"dstId")).as("edgeIdsInCluster")
      )
  }

  def mergePatternsByKMeansLabel(spark: SparkSession, clusteredNodes: DataFrame): DataFrame = {
    import spark.implicits._

    val mergedDF = clusteredNodes
      .withColumn("sortedLabels", array_sort($"labelsInCluster"))
      .groupBy($"sortedLabels")
      .agg(
        collect_list($"propertiesInCluster").as("propertiesInCluster"),
        flatten(collect_list($"nodeIdsInCluster")).as("nodeIdsInCluster")
      )

    mergedDF
      .withColumn("mandatoryProperties",
        aggregate($"propertiesInCluster", $"propertiesInCluster"(0), (acc, props) => array_intersect(acc, props))
      )
      .withColumn("allProperties", flatten($"propertiesInCluster"))
      .withColumn("optionalProperties", array_distinct(array_except($"allProperties", $"mandatoryProperties")))
      .drop("allProperties")
  }

  def mergeEdgePatternsByKMeansLabel(spark: SparkSession, clusteredEdges: DataFrame): DataFrame = {
    import spark.implicits._

    val mergedDF = clusteredEdges
      .withColumn("sortedRelationshipTypes", array_sort($"relsInCluster"))
      .groupBy($"sortedRelationshipTypes")
      .agg(
        collect_list($"propsInCluster").as("propsNested"),
        flatten(collect_list($"edgeIdsInCluster")).as("edgeIdsInCluster")
      )

    mergedDF
      .withColumn("propsInCluster", flatten($"propsNested"))
      .withColumn("mandatoryProperties",
        aggregate($"propsInCluster", $"propsInCluster"(0), (acc, props) => array_intersect(acc, props))
      )
      .withColumn("flattenedProps", flatten($"propsInCluster"))
      .withColumn("optionalProperties",
        array_distinct(array_except($"flattenedProps", $"mandatoryProperties"))
      )
      .select(
        $"sortedRelationshipTypes".as("relationshipTypes"),
        $"propsInCluster",
        $"edgeIdsInCluster",
        $"mandatoryProperties",
        $"optionalProperties"
      )
  }

}
