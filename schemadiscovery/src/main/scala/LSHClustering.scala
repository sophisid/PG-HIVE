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

    val propCols = patternsDF.columns.filterNot(Seq("_nodeId", "_labels", "originalLabels").contains)
    val nonNullProps = array(
      propCols.map(c => when(col(c).isNotNull, lit(c)).otherwise(lit(null))).toSeq: _*
    ).as("nonNullProps")

    val clusteredDF = transformedDF
      .withColumn("nonNullProps", nonNullProps)
      .groupBy($"hashes")
      .agg(
        collect_set($"_labels").as("labelsInCluster"),
        collect_set(filter($"nonNullProps", x => x.isNotNull)).as("propertiesInCluster"),
        collect_list($"_nodeId").as("nodeIdsInCluster")
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

    val propCols = df.columns.filter(_.startsWith("prop_"))

    val nonNullProps = array(
      propCols.map(c => when(col(c).isNotNull, lit(c)).otherwise(lit(null))).toSeq: _*
    ).as("nonNullProps")

    val groupedDF = transformedDF
      .withColumn("nonNullProps", nonNullProps)
      .groupBy($"hashes")
      .agg(
        collect_set($"relationshipType").as("relsInCluster"),
        collect_set($"srcType").as("srcLabelsInCluster"),
        collect_set($"dstType").as("dstLabelsInCluster"),
        collect_set(filter($"nonNullProps", x => x.isNotNull)).as("propsInCluster"),
        collect_list(struct($"srcId", $"dstId")).as("edgeIdsInCluster")
      )

    groupedDF
  }

def mergePatternsByLabel(spark: SparkSession, clusteredNodes: DataFrame): DataFrame = {
  import spark.implicits._

  clusteredNodes.cache()

  val withLabelsDF = clusteredNodes.filter(size($"labelsInCluster") > 0)
  val noLabelsDF = clusteredNodes.filter(size($"labelsInCluster") === 0)

  println(s"Number of node clusters before merge: ${clusteredNodes.count()}")

  // Merge για clusters με labels (αμετάβλητο)
  val mergedWLabelDF = withLabelsDF
    .withColumn("sortedLabels", array_sort($"labelsInCluster"))
    .groupBy($"sortedLabels")
    .agg(
      collect_list($"propertiesInCluster").as("propertiesInCluster"),
      flatten(collect_list($"nodeIdsInCluster")).as("nodeIdsInCluster")
    )

  val finalDF = mergedWLabelDF
    .withColumn("mandatoryProperties",
      aggregate(
        $"propertiesInCluster",
        $"propertiesInCluster"(0),
        (acc, props) => array_intersect(acc, props)
      )
    )
    .withColumn("allProperties", flatten($"propertiesInCluster"))
    .withColumn("optionalProperties",
      array_distinct(array_except($"allProperties", $"mandatoryProperties"))
    )
    .drop("allProperties")

  // Διορθωμένη διαχείριση clusters χωρίς labels
  val noLabelsFinalDF = noLabelsDF
    .select(
      $"labelsInCluster".as("sortedLabels"),
      array($"propertiesInCluster").as("propertiesInCluster"), // Προσαρμόζουμε σε Array[Array[Array[String]]]
      $"nodeIdsInCluster"
    )
    .withColumn("mandatoryProperties", flatten($"propertiesInCluster")) // Array[String]
    .withColumn("optionalProperties", array()) // Array[String]

  // Ένωση των δύο DataFrames
  val returnedDF = finalDF.union(noLabelsFinalDF)

  returnedDF
}

// do the same for edges
def mergeEdgePatternsByLabel(spark: SparkSession, clusteredEdges: DataFrame): DataFrame = {
  import spark.implicits._

  clusteredEdges.cache()

  val filteredDF = clusteredEdges
    .filter(size($"relsInCluster") > 0)

  println(s"Number of edge clusters before merge: ${filteredDF.count()}")

  val mergedDF = filteredDF
    .withColumn("sortedRelationshipTypes", array_sort($"relsInCluster"))
    .withColumn("sortedSrcLabels", array_sort($"srcLabelsInCluster"))
    .withColumn("sortedDstLabels", array_sort($"dstLabelsInCluster"))
    .groupBy($"sortedRelationshipTypes", $"sortedSrcLabels", $"sortedDstLabels")
    .agg(
      collect_list($"propsInCluster").as("propsNested"),
      flatten(collect_list($"edgeIdsInCluster")).as("edgeIdsInCluster")
    )

  val finalDF = mergedDF
    .withColumn("propsInCluster", flatten($"propsNested"))
    .withColumn("mandatoryProperties",
      aggregate(
        $"propsInCluster",
        $"propsInCluster"(0),
        (acc, props) => array_intersect(acc, props)
      )
    )
    .withColumn("flattenedProps", flatten($"propsInCluster"))
    .withColumn("optionalProperties",
      array_distinct(array_except($"flattenedProps", $"mandatoryProperties"))
    )
    .select(
      $"sortedRelationshipTypes".as("relationshipTypes"),
      $"sortedSrcLabels".as("srcLabels"),
      $"sortedDstLabels".as("dstLabels"),
      $"propsInCluster",
      $"edgeIdsInCluster",
      $"mandatoryProperties",
      $"optionalProperties"
    )

  finalDF
}
}