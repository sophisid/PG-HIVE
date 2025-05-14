import org.apache.spark.ml.feature.BucketedRandomProjectionLSH
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.ml.linalg.{Vector, Vectors}
import scala.math.sqrt

object LSHClustering {

  def estimateLSHParams(
    df: DataFrame, 
    featuresCol: String, 
    isEdge: Boolean = false, 
    sampleSize: Int = 10000, 
    uniqueLabelCount: Option[Long] = None 
  ): (Double, Int) = {
    import df.sparkSession.implicits._

    val limitVal = math.min(sampleSize, df.count().toInt)
    val sampledDF = df.sample(false, limitVal.toDouble / df.count(), 42).limit(limitVal).cache()
    // println(s"Sampled ${sampledDF.count()} rows for LSH parameter estimation")

    val featurePairs = sampledDF.crossJoin(sampledDF.withColumnRenamed(featuresCol, "features2"))
      .select(col(featuresCol).as("f1"), col("features2").as("f2"))
      .filter(col("f1") =!= col("f2"))
      .limit(limitVal)

    val distanceUdf = udf((v1: Vector, v2: Vector) => {
      val diff = v1.toArray.zip(v2.toArray).map { case (x, y) => (x - y) * (x - y) }
      sqrt(diff.sum)
    })

    val distances = featurePairs
      .withColumn("distance", distanceUdf(col("f1"), col("f2")))
      .agg(
        avg("distance").as("avgDistance"),
        min("distance").as("minDistance"),
        max("distance").as("maxDistance"),
        stddev("distance").as("stddevDistance")
      )
      .collect()(0)

    val avgDistance = distances.getAs[Double]("avgDistance")
    val minDistance = distances.getAs[Double]("minDistance")
    val maxDistance = distances.getAs[Double]("maxDistance")
    val stddevDistance = distances.getAs[Double]("stddevDistance")

    println(s"Avg Distance: $avgDistance, Min Distance: $minDistance, Max Distance: $maxDistance, StdDev: $stddevDistance")

    val (bucketLength, numHashTables) = if (isEdge) {
      val edgeBucketLength = avgDistance * 0.2
      val edgeNumHashTables = math.min(8, math.max(3, (df.count() / 20000).toInt))
      (edgeBucketLength, edgeNumHashTables)
    } else {
      val baseNodeBucketLength = avgDistance * 0.3
      val baseNodeNumHashTables = math.min(10, math.max(5, (df.count() / 30000).toInt))

      uniqueLabelCount match {
        case Some(labelCount) =>
          val bucketLengthAdjustment = labelCount match {
            case n if n <= 3  => 0.5
            case n if n <= 10 => 1.0
            case _            => 1.5
          }
          val numHashTablesAdjustment = labelCount match {
            case n if n <= 3  => 1.5 
            case n if n <= 10 => 1.0
            case _            => 0.7
          }

          val adjustedBucketLength = baseNodeBucketLength * bucketLengthAdjustment
          val adjustedNumHashTables = math.max(5, (baseNodeNumHashTables * numHashTablesAdjustment).toInt)

          (adjustedBucketLength, adjustedNumHashTables)
        case None =>
          (baseNodeBucketLength, baseNodeNumHashTables)
      }
    }

    println(s"Estimated bucketLength: $bucketLength")
    println(s"Estimated numHashTables: $numHashTables")

    sampledDF.unpersist()

    (bucketLength, numHashTables)
  }

  def applyLSHNodes(spark: SparkSession, patternsDF: DataFrame): DataFrame = {
    import spark.implicits._

    if (patternsDF.isEmpty) {
      println("No patterns to cluster.")
      return spark.emptyDataFrame
    }
    val uniqueLabelCount = patternsDF
      .select(explode(split(col("_labels"), ",")))
      .distinct()
      .count()
    println(s"Number of unique labels: $uniqueLabelCount")
    val (rawBucketLength, rawNumHashTables) = estimateLSHParams(patternsDF, "features", isEdge = false, uniqueLabelCount = Some(uniqueLabelCount))
    val bucketLength = if (rawBucketLength == 0.0) 0.2 else rawBucketLength
    val numHashTables = if (rawNumHashTables == 0) 5 else rawNumHashTables
    val lsh = new BucketedRandomProjectionLSH()
      .setBucketLength(bucketLength)
      .setNumHashTables(numHashTables)
      .setInputCol("features")
      .setOutputCol("hashes")

    val model = lsh.fit(patternsDF)
    val transformedDF = model.transform(patternsDF)

    val originalPropCols = patternsDF.columns
      .filterNot(_.startsWith("prop_"))
      .filterNot(Seq("_nodeId", "_labels", "originalLabels", "labelArray", "labelVector", "features", "hashes").contains)
    
    val nonNullProps = array(
      originalPropCols.map(c => when(col(c).isNotNull, lit(c)).otherwise(lit(null))).toSeq: _*
    ).as("nonNullProps")

    val clusteredDF = transformedDF
      .withColumn("nonNullProps", nonNullProps)
      .groupBy($"hashes")
      .agg(
        collect_set($"_labels").as("labelsInCluster"),
        collect_set(filter($"nonNullProps", x => x.isNotNull)).as("propertiesInCluster"),
        collect_list($"_nodeId").as("nodeIdsInCluster")
      )
      .withColumn("row_num", row_number().over(Window.orderBy($"hashes")))
      .withColumn("cluster_id", concat(lit("cluster_node_"), $"row_num"))
      .drop("hashes", "row_num", "features", "labelVector", "labelArray", "nonNullProps")
      .drop(patternsDF.columns.filter(_.startsWith("prop_")): _*)

    println("Schema after LSH:")
    clusteredDF.printSchema()
    println("Sample after LSH:")
    clusteredDF.show(50)

    clusteredDF
  }

  def applyLSHEdges(spark: SparkSession, df: DataFrame): DataFrame = {
    import spark.implicits._

    if (df.isEmpty) {
      println("No edge patterns to cluster.")
      return spark.emptyDataFrame
    }

    val (rawBucketLength, rawNumHashTables) = estimateLSHParams(df, "features", isEdge = true)
    val bucketLength = if (rawBucketLength == 0.0) 0.2 else rawBucketLength
    val numHashTables = if (rawNumHashTables == 0) 5 else rawNumHashTables

    val lsh = new BucketedRandomProjectionLSH()
      .setBucketLength(bucketLength)
      .setNumHashTables(numHashTables)
      .setInputCol("features")
      .setOutputCol("hashes")

    val model = lsh.fit(df)
    val transformedDF = model.transform(df)

    val originalPropCols = df.columns
      .filterNot(_.startsWith("prop_")) 
      .filterNot(Seq("srcType", "srcId", "relationshipType", "dstType", "dstId", "relationshipTypeArray", "srcLabelArray", "dstLabelArray", "relVector", "srcVector", "dstVector", "features", "hashes").contains)

    val nonNullProps = array(
      originalPropCols.map(c => when(col(c).isNotNull, lit(c)).otherwise(lit(null))).toSeq: _*
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
      .withColumn("row_num", row_number().over(Window.orderBy($"hashes")))
      .withColumn("cluster_id", concat(lit("cluster_edge_"), $"row_num"))
      .drop("hashes", "row_num", "features", "relVector", "srcVector", "dstVector", "relationshipTypeArray", "srcLabelArray", "dstLabelArray", "nonNullProps")
      .drop(df.columns.filter(_.startsWith("prop_")): _*)

    println("Schema after LSH for edges:")
    groupedDF.printSchema()
    println("Sample after LSH for edges:")
    groupedDF.show(50)

    groupedDF
  }

  def mergeClustersByJaccard(df: DataFrame, similarityThreshold: Double = 0.9): DataFrame = {
    import df.sparkSession.implicits._

    val jaccardUdf = udf((labels1: Seq[String], labels2: Seq[String]) => {
      val set1 = labels1.toSet
      val set2 = labels2.toSet
      val intersection = set1.intersect(set2).size
      val union = set1.union(set2).size
      if (union == 0) 0.0 else intersection.toDouble / union
    })

    val clusterPairs = df.crossJoin(df.withColumnRenamed("cluster_id", "cluster_id2")
      .withColumnRenamed("labelsInCluster", "labelsInCluster2")
      .withColumnRenamed("propertiesInCluster", "propertiesInCluster2")
      .withColumnRenamed("nodeIdsInCluster", "nodeIdsInCluster2"))
      .filter($"cluster_id" < $"cluster_id2")
      .withColumn("jaccard_similarity", jaccardUdf($"labelsInCluster", $"labelsInCluster2"))

    val similarPairs = clusterPairs
      .filter($"jaccard_similarity" >= similarityThreshold)
      .select($"cluster_id", $"cluster_id2")

    if (similarPairs.isEmpty) {
      println("No clusters to merge based on Jaccard similarity.")
      return df
    }

    val mergeGroups = similarPairs
      .groupBy($"cluster_id")
      .agg(collect_set($"cluster_id2").as("merge_with"))
      .withColumn("all_clusters", array_union(array($"cluster_id"), $"merge_with"))
      .select($"all_clusters")
      .distinct()

    val toMerge = df.join(mergeGroups, array_contains($"all_clusters", $"cluster_id"), "left_outer")
      .groupBy($"all_clusters")
      .agg(
        flatten(collect_set($"labelsInCluster")).as("labelsInCluster"),
        flatten(collect_set($"propertiesInCluster")).as("propertiesInCluster"),
        flatten(collect_set($"nodeIdsInCluster")).as("nodeIdsInCluster")
      )
      .filter($"all_clusters".isNotNull)
      .withColumn("cluster_id", concat(lit("merged_jaccard_node_"), monotonically_increasing_id()))

    val unmerged = df.join(mergeGroups, array_contains($"all_clusters", $"cluster_id"), "left_anti")

    val result = unmerged.select("cluster_id", "labelsInCluster", "propertiesInCluster", "nodeIdsInCluster")
      .union(toMerge.select("cluster_id", "labelsInCluster", "propertiesInCluster", "nodeIdsInCluster"))

    println(s"Merged clusters into ${result.count()} total clusters using Jaccard similarity.")
    result
  }
  def mergePatternsByLabel(spark: SparkSession, clusteredNodes: DataFrame): DataFrame = {
    import spark.implicits._

    // println(s"Initial clusters: ${clusteredNodes.count()}")
    // clusteredNodes.printSchema()

    val cleanedClusters = clusteredNodes
      .withColumn("cleanedLabels", array_distinct(filter($"labelsInCluster", label => label =!= "" && label.isNotNull)))
      .withColumn("sortedLabels", array_sort($"cleanedLabels"))

    // println("Sample of cleanedClusters:")
    // cleanedClusters.select("cluster_id", "labelsInCluster", "cleanedLabels", "sortedLabels").show(20)

    val withLabelsDF = cleanedClusters.filter(size($"cleanedLabels") > 0)
    val noLabelsDF = cleanedClusters.filter(size($"cleanedLabels") === 0)

    // println(s"Clusters with labels: ${withLabelsDF.count()}")
    // println(s"Clusters without labels: ${noLabelsDF.count()}")

    val mergedWithLabels = withLabelsDF
      .groupBy($"sortedLabels")
      .agg(
        flatten(collect_list($"nodeIdsInCluster")).as("nodeIdsInCluster"),
        collect_set($"cluster_id").as("original_cluster_ids"),
        flatten(flatten(collect_list($"propertiesInCluster"))).as("allProperties"),
        collect_list($"propertiesInCluster").as("propertiesNested")
      )
      .withColumn("propertiesInCluster", array_distinct($"allProperties"))
      .withColumn("mandatoryProperties",
        array_distinct(aggregate(
          $"propertiesNested",
          array().cast("array<string>"),
          (acc, props) => when(size(acc) === 0, flatten(props)).otherwise(array_intersect(acc, flatten(props)))
        ))
      )
      .withColumn("optionalProperties",
        array_distinct(array_except($"allProperties", $"mandatoryProperties")))
      .withColumn("merged_cluster_id", concat(lit("merged_with_label_"), monotonically_increasing_id()))
      .drop("allProperties", "propertiesNested")

    // println("Merged labeled clusters (grouped by identical labels):")
    // mergedWithLabels.show(50)

    val jaccardUdf = udf((props1: Seq[String], props2: Seq[String]) => {
      val set1 = props1.toSet
      val set2 = props2.toSet
      val intersection = set1.intersect(set2).size
      val union = set1.union(set2).size
      if (union == 0) 0.0 else intersection.toDouble / union
    })

    val noLabelsWithMatchTemp = noLabelsDF
      .crossJoin(mergedWithLabels.select(
        $"sortedLabels".as("labeledLabels"),
        $"propertiesInCluster".as("labeledProps"),
        $"mandatoryProperties".as("labeledMandatoryProps"),
        $"optionalProperties".as("labeledOptionalProps"),
        $"merged_cluster_id".as("existing_cluster_id")
      ))
      .withColumn("jaccard_similarity", jaccardUdf(flatten($"propertiesInCluster"), $"labeledProps"))
      .filter($"jaccard_similarity" >= 0.9)
      .select(
        $"labeledLabels".as("matchedSortedLabels"),
        $"nodeIdsInCluster".as("matchedNodeIds"),
        array($"cluster_id").as("matchedOriginalIds"),
        flatten($"propertiesInCluster").as("matchedProperties"),
        $"labeledMandatoryProps".as("matchedMandatoryProperties"),
        $"labeledOptionalProps".as("matchedOptionalProperties"),
        $"existing_cluster_id".as("merged_cluster_id")
      )

    val mergedWithLabelsUpdated = mergedWithLabels
      .join(noLabelsWithMatchTemp, Seq("merged_cluster_id"), "left_outer")
      .groupBy($"merged_cluster_id")
      .agg(
        first(mergedWithLabels("sortedLabels")).as("sortedLabels"),
        flatten(collect_list(coalesce($"matchedNodeIds", $"nodeIdsInCluster"))).as("nodeIdsInCluster"),
        flatten(collect_set(coalesce($"matchedOriginalIds", $"original_cluster_ids"))).as("original_cluster_ids"),
        array_distinct(flatten(collect_list(coalesce($"matchedProperties", $"propertiesInCluster")))).as("propertiesInCluster"),
        array_distinct(flatten(collect_list(mergedWithLabels("mandatoryProperties")))).as("mandatoryProperties"),
        array_distinct(flatten(collect_list(coalesce($"matchedOptionalProperties", mergedWithLabels("optionalProperties"))))).as("optionalProperties")
      )

    // println("Merged labeled clusters with matched unlabeled:")
    // mergedWithLabelsUpdated.show(50)

    val noLabelsUnmatched = noLabelsDF
      .join(noLabelsWithMatchTemp.select($"matchedOriginalIds").withColumnRenamed("matchedOriginalIds", "matched_ids"),
        array_contains($"matched_ids", $"cluster_id"), "left_anti")
      .select(
        $"cluster_id".as("merged_cluster_id"),
        $"sortedLabels",
        transform($"nodeIdsInCluster", x => x.cast("string")).as("nodeIdsInCluster"),
        array($"cluster_id").as("original_cluster_ids"),
        flatten($"propertiesInCluster").as("propertiesInCluster"),
        flatten($"propertiesInCluster").as("mandatoryProperties"),
        array().cast("array<string>").as("optionalProperties")
      )

    // println("Unmatched unlabeled clusters:")
    // noLabelsUnmatched.show(50)
    // mergedWithLabelsUpdated.printSchema()
    // noLabelsUnmatched.printSchema()
    val finalDF = mergedWithLabelsUpdated
      .union(noLabelsUnmatched)

    // println(s"Clusters after merging: ${finalDF.count()}")
    // println("Schema after merging:")
    // finalDF.printSchema()
    // println("Sample after merging:")
    // finalDF.show(20)

    finalDF
  }

  def mergeEdgePatternsByLabel(spark: SparkSession, clusteredEdges: DataFrame): DataFrame = {
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
      .withColumn("row_num", row_number().over(Window.orderBy($"sortedRelationshipTypes", $"sortedSrcLabels", $"sortedDstLabels")))
      .withColumn("merged_cluster_id", concat(lit("merged_cluster_edge_"), $"row_num"))
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
        flatten($"propsInCluster").as("mandatoryProperties"),
        array().cast("array<string>").as("optionalProperties"),
        array($"cluster_id").as("original_cluster_ids"),
        $"cluster_id".as("merged_cluster_id")
      )

    println("Schema of finalDF:")
    finalDF.printSchema()
    println("Schema of noLabelsFinalDF:")
    noLabelsFinalDF.printSchema()

    val returnedDF = finalDF.union(noLabelsFinalDF)
    returnedDF
  }
//simple merge only
  def mergeEdgePatternsByEdgeLabel(spark: SparkSession, clusteredEdges: DataFrame): DataFrame = {
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

    println(s"Number of edge clusters before merge by edge label: ${clusteredEdges.count()}")

    val mergedDF = withLabelsDF
      .withColumn("sortedRelationshipTypes", array_sort($"relsInCluster"))
      .groupBy($"sortedRelationshipTypes")
      .agg(
        flatten(collect_set($"srcLabelsInCluster")).as("srcLabels"),
        flatten(collect_set($"dstLabelsInCluster")).as("dstLabels"),
        collect_list($"propsInCluster").as("propsNested"),
        flatten(collect_list($"edgeIdsInCluster")).as("edgeIdsInCluster"),
        collect_set($"cluster_id").as("original_cluster_ids")
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
      .withColumn("row_num", row_number().over(Window.orderBy($"sortedRelationshipTypes")))
      .withColumn("merged_cluster_id", concat(lit("merged_cluster_edge_by_label_"), $"row_num"))
      .select(
        $"sortedRelationshipTypes".as("relationshipTypes"),
        $"srcLabels",
        $"dstLabels",
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
        flatten($"propsInCluster").as("mandatoryProperties"),
        array().cast("array<string>").as("optionalProperties"),
        array($"cluster_id").as("original_cluster_ids"),
        $"cluster_id".as("merged_cluster_id")
      )

    println("Schema of finalDF (by edge label):")
    finalDF.printSchema()
    println("Schema of noLabelsFinalDF (by edge label):")
    noLabelsFinalDF.printSchema()

    val returnedDF = finalDF.union(noLabelsFinalDF)
    returnedDF
  }
}