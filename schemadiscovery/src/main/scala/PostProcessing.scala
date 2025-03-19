import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object PostProcessing {

  def groupPatternsByLabel(finalClusteredNodesDF: DataFrame): DataFrame = {
    import finalClusteredNodesDF.sparkSession.implicits._

    val explodedDF = finalClusteredNodesDF
      .withColumn("flatLabels", flatten($"labelsInCluster"))
      .withColumn("label", explode($"flatLabels"))
      .withColumn("flatProperties", flatten($"propertiesInCluster"))
      .select("hashes", "label", "flatProperties")

    val groupedDF = explodedDF
      .groupBy("label")
      .agg(collect_list($"flatProperties").as("allProperties"))
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

        (label, commonProperties, optionalProperties)
      }
      .toDF("label", "nonOptionalProperties", "optionalProperties")

    groupedDF
  }

def groupPatternsByEdgeType(finalClusteredEdgesDF: DataFrame): DataFrame = {
    import finalClusteredEdgesDF.sparkSession.implicits._

    val flattenedDF = finalClusteredEdgesDF
      .withColumn("flatRels", flatten($"relsInCluster"))
      .withColumn("flatSrc",  flatten($"srcLabelsInCluster"))
      .withColumn("flatDst",  flatten($"dstLabelsInCluster"))
      .withColumn("flatProps", flatten($"propsInCluster"))

    val sortedDF = flattenedDF
      .withColumn("sortedRels", array_sort($"flatRels"))
      .withColumn("sortedSrc",  array_sort($"flatSrc"))
      .withColumn("sortedDst",  array_sort($"flatDst"))

    val grouped = sortedDF
      .groupBy($"sortedRels", $"sortedSrc", $"sortedDst")
      .agg(
        collect_list($"flatProps").as("allProperties")
      )

    val result = grouped.map { row =>
      val rels = row.getAs[Seq[String]]("sortedRels")
      val src  = row.getAs[Seq[String]]("sortedSrc")
      val dst  = row.getAs[Seq[String]]("sortedDst")
      val allPropsNested = row.getAs[Seq[Seq[String]]]("allProperties")
      val allPropsFlattened = allPropsNested.flatten

      val numberOfSubArrays = allPropsNested.size

      val commonProperties = allPropsFlattened
        .groupBy(identity)
        .mapValues(_.size)
        .filter { case (_, count) => count == numberOfSubArrays }
        .keys
        .toSeq
        .sorted

      val optionalProperties = allPropsFlattened.distinct.diff(commonProperties).sorted

      (
        rels.mkString(";"),
        src.mkString(";"), 
        dst.mkString(";"),
        commonProperties,
        optionalProperties
      )
    }.toDF(
      "relationshipTypes", 
      "srcLabels",
      "dstLabels",
      "nonOptionalProperties",
      "optionalProperties"
    )

    result
  }

}
