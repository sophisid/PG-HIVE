import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

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

    val propCols = patternsDF.columns.filterNot(Seq("_nodeId", "_labels", "features", "originalLabels").contains)
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
      .withColumn("cluster_id", concat(lit("cluster_"), $"cluster"))
      .drop("cluster")
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
      .withColumn("cluster_id", concat(lit("cluster_"), $"cluster"))
      .drop("cluster")
  }

  def mergePatternsByKMeansLabel(spark: SparkSession, clusteredNodes: DataFrame): DataFrame = {
  import spark.implicits._

  clusteredNodes.cache()

  val withLabelsDF = clusteredNodes.filter(size($"labelsInCluster") > 0)
  val noLabelsDF = clusteredNodes.filter(size($"labelsInCluster") === 0)

  println(s"Number of node clusters before merge: ${clusteredNodes.count()}")

  val mergedWLabelDF = withLabelsDF
    .withColumn("sortedLabels", array_sort($"labelsInCluster"))
    .groupBy($"sortedLabels")
    .agg(
      collect_list($"propertiesInCluster").as("propertiesInCluster"),
      flatten(collect_list($"nodeIdsInCluster")).as("nodeIdsInCluster"),
      collect_set($"cluster_id").as("original_cluster_ids")
    )

  val finalDF = mergedWLabelDF
    .withColumn("mandatoryProperties",
      flatten(aggregate( // Add flatten to ensure Array[String]
        $"propertiesInCluster",
        $"propertiesInCluster"(0),
        (acc, props) => array_intersect(acc, props)
      ))
    )
    .withColumn("allProperties", flatten(flatten($"propertiesInCluster"))) // Double flatten to match Array[String]
    .withColumn("optionalProperties",
      array_distinct(array_except($"allProperties", $"mandatoryProperties"))
    )
    .drop("allProperties")
    .withColumn("row_num", row_number().over(Window.orderBy($"sortedLabels")))
    .withColumn("merged_cluster_id", concat(lit("merged_cluster_node_"), $"row_num"))
    .drop("row_num")

  val noLabelsFinalDF = noLabelsDF
    .select(
      $"labelsInCluster".as("sortedLabels"),
      array($"propertiesInCluster").as("propertiesInCluster"),
      $"nodeIdsInCluster",
      flatten($"propertiesInCluster").as("mandatoryProperties"), // Flatten to Array[String]
      array().cast("array<string>").as("optionalProperties"), // Ensure Array[String]
      array($"cluster_id").as("original_cluster_ids"), // Consistent with finalDF
      $"cluster_id".as("merged_cluster_id")
    )

  // Debugging: Εκτύπωση schemas πριν το union
  println("Schema of finalDF:")
  finalDF.printSchema()
  println("Schema of noLabelsFinalDF:")
  noLabelsFinalDF.printSchema()

  val returnedDF = finalDF.union(noLabelsFinalDF)
  returnedDF
}

 def mergeEdgePatternsByKMeansLabel(spark: SparkSession, clusteredEdges: DataFrame): DataFrame = {
  import spark.implicits._

  clusteredEdges.cache()

  val withLabelsDF = clusteredEdges.filter(
    (size($"relsInCluster") > 0) &&
    (size($"srcLabelsInCluster") > 0 || size($"dstLabelsInCluster") > 0)
  )
  val noLabelsDF = clusteredEdges.filter(
    (size($"relsInCluster") > 0) &&
    (size($"srcLabelsInCluster") === 0 && size($"dstLabelsInCluster") === 0)
  )

  println(s"Number of edge clusters before merge: ${clusteredEdges.count()}")

  val mergedDF = withLabelsDF
    .withColumn("sortedRelationshipTypes", array_sort($"relsInCluster"))
    .withColumn("sortedSrcLabels", array_sort($"srcLabelsInCluster"))
    .withColumn("sortedDstLabels", array_sort($"dstLabelsInCluster"))
    .groupBy($"sortedRelationshipTypes", $"sortedSrcLabels", $"sortedDstLabels")
    .agg(
      collect_list($"propsInCluster").as("propsNested"),
      flatten(collect_list($"edgeIdsInCluster")).as("edgeIdsInCluster"),
      collect_set($"cluster_id").as("original_cluster_ids")
    )

  val finalDF = mergedDF
    .withColumn("propsInCluster", flatten($"propsNested")) // Array[String]
    .withColumn("mandatoryProperties",
      aggregate( // No flatten needed, already Array[String]
        $"propsInCluster",
        $"propsInCluster"(0),
        (acc, props) => array_intersect(acc, props)
      )
    )
    .withColumn("flattenedProps", flatten($"propsInCluster")) // Single flatten, already Array[String]
    .withColumn("optionalProperties",
      array_distinct(array_except($"flattenedProps", $"mandatoryProperties"))
    )
    .withColumn("row_num", row_number().over(Window.orderBy($"sortedRelationshipTypes", $"sortedSrcLabels", $"sortedDstLabels")))
    .withColumn("merged_cluster_id", concat(lit("merged_cluster_"), $"row_num"))
    .select(
      $"sortedRelationshipTypes".as("relationshipTypes"),
      $"sortedSrcLabels".as("srcLabels"),
      $"sortedDstLabels".as("dstLabels"),
      $"propsInCluster",
      $"edgeIdsInCluster",
      $"mandatoryProperties",
      $"optionalProperties",
      $"original_cluster_ids",
      $"merged_cluster_id"
    )
    .drop("flattenedProps", "row_num")

  val noLabelsFinalDF = noLabelsDF
    .select(
      $"relsInCluster".as("relationshipTypes"),
      $"srcLabelsInCluster".as("srcLabels"),
      $"dstLabelsInCluster".as("dstLabels"),
      $"propsInCluster",
      $"edgeIdsInCluster",
      flatten($"propsInCluster").as("mandatoryProperties"), // Flatten to Array[String]
      array().cast("array<string>").as("optionalProperties"), // Ensure Array[String]
      array($"cluster_id").as("original_cluster_ids"), // Wrap in array for consistency
      $"cluster_id".as("merged_cluster_id")
    )

  // Debugging: Εκτύπωση schemas πριν το union
  println("Schema of finalDF:")
  finalDF.printSchema()
  println("Schema of noLabelsFinalDF:")
  noLabelsFinalDF.printSchema()

  val returnedDF = finalDF.union(noLabelsFinalDF)
  returnedDF
}
}