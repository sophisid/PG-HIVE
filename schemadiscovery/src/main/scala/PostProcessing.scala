import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object PostProcessing {

  def groupPatternsByLabel(finalClusteredNodesDF: DataFrame): DataFrame = {
    import finalClusteredNodesDF.sparkSession.implicits._

    val explodedDF = finalClusteredNodesDF
      .withColumn("flatLabels", flatten($"labelsInCluster"))
      .withColumn("label", explode($"flatLabels"))
      .withColumn("flatProperties", flatten($"propertiesInCluster"))
      .withColumn("nodeId", explode($"nodeIdsInCluster"))
      .select("hashes", "label", "flatProperties", "nodeId")

    val groupedDF = explodedDF
      .groupBy("label")
      .agg(collect_list($"flatProperties").as("allProperties"), collect_set($"nodeId").as("nodeIds"))
      .map { row =>
        val label = row.getAs[String]("label")
        val allPropsNested = row.getAs[Seq[Seq[String]]]("allProperties")
        val allPropsFlattened = allPropsNested.flatten

        val commonProperties = allPropsFlattened
          .groupBy(identity)
          .mapValues(_.size)
          .filter { case (_, count) => count == allPropsNested.size }
          .keys
          .toSeq
          .sorted

        val optionalProperties = allPropsFlattened.distinct.diff(commonProperties).sorted
        val nodeIds = row.getAs[Seq[Long]]("nodeIds")
        val allProperties = (commonProperties ++ optionalProperties).distinct.sorted

        (label, commonProperties, optionalProperties, allProperties, nodeIds) 
      }
      .toDF("label", "nonOptionalProperties", "optionalProperties", "allProperties", "nodeIds")

    groupedDF
  }

def groupPatternsByEdgeType(finalClusteredEdgesDF: DataFrame): DataFrame = {
    import finalClusteredEdgesDF.sparkSession.implicits._

    val flattenedDF = finalClusteredEdgesDF
      .withColumn("flatRels", flatten($"relsInCluster"))
      .withColumn("flatSrc",  flatten($"srcLabelsInCluster"))
      .withColumn("flatDst",  flatten($"dstLabelsInCluster"))
      .withColumn("flatProps", flatten($"propsInCluster"))
      .withColumn("edgeId", explode($"edgeIdsInCluster"))

    val sortedDF = flattenedDF
      .withColumn("sortedRels", array_sort($"flatRels"))
      .withColumn("sortedSrc",  array_sort($"flatSrc"))
      .withColumn("sortedDst",  array_sort($"flatDst"))

    val grouped = sortedDF
      .groupBy($"sortedRels", $"sortedSrc", $"sortedDst")
      .agg(
        collect_list($"flatProps").as("allProperties"),
        collect_set($"edgeId").as("edgeIds")
      )

    val result = grouped.map { row =>
      val rels = row.getAs[Seq[String]]("sortedRels")
      val src  = row.getAs[Seq[String]]("sortedSrc")
      val dst  = row.getAs[Seq[String]]("sortedDst")
      val allPropsNested = row.getAs[Seq[Seq[String]]]("allProperties")
      val allPropsFlattened = allPropsNested.flatten
      val edgeIds = row.getAs[Seq[Long]]("edgeIds")

      val numberOfSubArrays = allPropsNested.size

      val commonProperties = allPropsFlattened
        .groupBy(identity)
        .mapValues(_.size)
        .filter { case (_, count) => count == numberOfSubArrays }
        .keys
        .toSeq
        .sorted

      val optionalProperties = allPropsFlattened.distinct.diff(commonProperties).sorted
      val allProperties = (commonProperties ++ optionalProperties).distinct.sorted

      (
        rels.mkString(";"),
        src.mkString(";"), 
        dst.mkString(";"),
        commonProperties,
        optionalProperties,
        allProperties,
        edgeIds
      )
    }.toDF(
      "relationshipTypes", 
      "srcLabels",
      "dstLabels",
      "nonOptionalProperties",
      "optionalProperties",
      "allProperties",
      "edgeIds"
    )

    result
  }

}
