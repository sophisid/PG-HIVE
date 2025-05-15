import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object Evaluation {

def computeMetricsForNodes(
    spark: SparkSession,
    originalNodesDF: DataFrame,
    predictedNodesDF: DataFrame
  ): Unit = {
    import spark.implicits._

    if (!originalNodesDF.columns.contains("original_label")) {
      println("[ERROR] Column 'original_label' is missing from the originalNodesDF. Please include it before running evaluation.")
      return
    }

    val explodedPredictedDF = predictedNodesDF
      .withColumn("nodeId", explode(col("nodeIdsInCluster")))
      .withColumn("nodeId", col("nodeId").cast("long"))
      .withColumn("predictedLabels", array_distinct(split(concat_ws(":", $"sortedLabels"), ":")))
      .select(col("nodeId"), col("predictedLabels"), col("merged_cluster_id"))
      .where(col("nodeId").isNotNull)

    val explodedOriginalDF = originalNodesDF
      .withColumn("actualLabels",
        when(col("original_label").isNotNull, split(col("original_label"), ","))
          .otherwise(array().cast("array<string>")))
      .select(col("_nodeId").as("nodeId"), col("actualLabels"))
      .where(col("nodeId").isNotNull)

    val evaluationDF = explodedPredictedDF
      .join(explodedOriginalDF, Seq("nodeId"), "left_outer")
      .select(col("nodeId"), col("predictedLabels"), col("actualLabels"), col("merged_cluster_id"))

    val distinctGroundTruthNodes = explodedOriginalDF.select(col("actualLabels")).distinct().count()
    val distinctPredictedNodes = predictedNodesDF.select(col("merged_cluster_id")).distinct().count()

    println(s"Ground Truth Nodes (distinct label sets): $distinctGroundTruthNodes")
    println(s"Predicted Nodes (distinct clusters): $distinctPredictedNodes")

    val evaluationNonStrictDF = evaluationDF
      .withColumn("correctAssignmentNonStrict",
        when(size(array_intersect(col("actualLabels"), col("predictedLabels"))) > 0, 1)
          .otherwise(0)
      )

    val evaluationWithCorrectnessDF = evaluationNonStrictDF
      .withColumn("correctAssignmentStrict",
        when(array_sort(col("actualLabels")) === array_sort(col("predictedLabels")), 1)
          .otherwise(0)
      )

    val labelFrequenciesDF = explodedPredictedDF
      .join(explodedOriginalDF, Seq("nodeId"), "inner")
      .withColumn("label", explode(col("actualLabels")))
      .groupBy("merged_cluster_id", "label")
      .agg(count("*").as("freq"))
      .withColumn("rank", row_number().over(Window.partitionBy("merged_cluster_id").orderBy(desc("freq"))))
      .filter($"rank" === 1)
      .select($"merged_cluster_id", $"label".as("majority_label"))

    val evaluationWithMajorityDF = evaluationWithCorrectnessDF
      .join(labelFrequenciesDF, Seq("merged_cluster_id"), "inner")
      .withColumn("correctAssignmentMajority",
        when(array_contains($"actualLabels", $"majority_label"), 1)
          .otherwise(0)
      )


    val totalActualPositivesDF = explodedOriginalDF
      .groupBy(col("actualLabels"))
      .agg(count("*").as("totalActual"))



    val TPMajority = evaluationWithMajorityDF.filter($"correctAssignmentMajority" === 1).count()
    val FPMajority = evaluationWithMajorityDF.filter($"correctAssignmentMajority" === 0).count()

    val totalPredictedPositivesMajorityDF = evaluationWithMajorityDF
      .filter($"correctAssignmentMajority" === 1)
      .groupBy(col("actualLabels"))
      .agg(count("*").as("totalPredicted"))

    val FNMajority = totalActualPositivesDF
      .join(totalPredictedPositivesMajorityDF, Seq("actualLabels"), "left_outer")
      .select(
        coalesce(col("totalActual"), lit(0L)).as("totalActual"),
        coalesce(col("totalPredicted"), lit(0L)).as("totalPredicted")
      )
      .withColumn("fnPerGroup", when(col("totalActual") > col("totalPredicted"), col("totalActual") - col("totalPredicted")).otherwise(lit(0L)))
      .agg(sum(col("fnPerGroup")).as("fnCount"))
      .first()
      .getLong(0)

    val precisionMajority = if (TPMajority + FPMajority > 0) TPMajority.toDouble / (TPMajority + FPMajority) else 0.0
    val recallMajority = if (TPMajority + FNMajority > 0) TPMajority.toDouble / (TPMajority + FNMajority) else 0.0
    val f1ScoreMajority = if (precisionMajority + recallMajority > 0) 2 * (precisionMajority * recallMajority) / (precisionMajority + recallMajority) else 0.0


    println(s"\nMajority Label Node Evaluation Metrics:")
    println(s"  True Positives (TP): $TPMajority")
    println(s"  False Positives (FP): $FPMajority")
    println(s"  False Negatives (FN): $FNMajority")
    println(f"  Precision: $precisionMajority%.4f")
    println(f"  Recall:    $recallMajority%.4f")
    println(f"  F1-Score:  $f1ScoreMajority%.4f")

    println("\nEvaluation Sample with Cluster IDs (Strict, Non-Strict, and Majority):")
    evaluationWithMajorityDF
      .select(
        col("nodeId"),
        col("predictedLabels"),
        col("actualLabels"),
        col("merged_cluster_id"),
        col("majority_label"),
        col("correctAssignmentNonStrict"),
        col("correctAssignmentStrict"),
        col("correctAssignmentMajority")
      )
      .show(10, false)
  }

  def computeMetricsForEdges(
    spark: SparkSession,
    originalEdgesDF: DataFrame,
    predictedEdgesDF: DataFrame
  ): Unit = {
    import spark.implicits._

    val requiredEdgeCols = Seq("relationshipType", "srcType", "dstType")
    val missingCols = requiredEdgeCols.filterNot(originalEdgesDF.columns.contains)

    if (missingCols.nonEmpty) {
      println(s"[ERROR] Missing required columns in originalEdgesDF: ${missingCols.mkString(", ")}. Please include them before running evaluation.")
      return
    }

    val explodedPredictedDF = predictedEdgesDF
      .withColumn("edgeId", explode(col("edgeIdsInCluster")))
      .select(
        struct(col("edgeId.srcId").as("srcId"), col("edgeId.dstId").as("dstId")).as("edgeId"),
        array_distinct(col("relationshipTypes")).as("predictedRelationshipTypes"),
        array_distinct(col("srcLabels")).as("predictedSrcLabels"),
        array_distinct(col("dstLabels")).as("predictedDstLabels"),
        col("merged_cluster_id")
      )

    val explodedOriginalDF = originalEdgesDF
      .withColumn("actualRelationshipTypes",
        when(col("relationshipType").isNotNull, array(col("relationshipType")))
          .otherwise(array().cast("array<string>")))
      .withColumn("actualSrcLabels",
        when(col("srcType").isNotNull, array(col("srcType")))
          .otherwise(array().cast("array<string>")))
      .withColumn("actualDstLabels",
        when(col("dstType").isNotNull, array(col("dstType")))
          .otherwise(array().cast("array<string>")))
      .select(
        struct(col("srcId"), col("dstId")).as("edgeId"),
        col("actualRelationshipTypes"),
        col("actualSrcLabels"),
        col("actualDstLabels")
      )

    val evaluationDF = explodedPredictedDF
      .join(explodedOriginalDF, "edgeId", "inner")
      .select(
        col("edgeId"),
        col("predictedRelationshipTypes"),
        col("predictedSrcLabels"),
        col("predictedDstLabels"),
        col("actualRelationshipTypes"),
        col("actualSrcLabels"),
        col("actualDstLabels"),
        col("merged_cluster_id")
      )

    val distinctGroundTruthEdges = explodedOriginalDF
      .select(col("actualRelationshipTypes"), col("actualSrcLabels"), col("actualDstLabels"))
      .distinct()
      .count()
    val distinctPredictedEdges = predictedEdgesDF
      .select(col("merged_cluster_id"))
      .distinct()
      .count()

    println(s"Ground Truth Edges (distinct): $distinctGroundTruthEdges")
    println(s"Predicted Edges (distinct clusters): $distinctPredictedEdges")

    val evaluationNonStrictDF = evaluationDF
      .withColumn("correctAssignmentNonStrict",
        when(
          size(array_except(col("actualRelationshipTypes"), col("predictedRelationshipTypes"))) === 0 &&
          size(array_except(col("actualSrcLabels"), col("predictedSrcLabels"))) === 0 &&
          size(array_except(col("actualDstLabels"), col("predictedDstLabels"))) === 0,
          1
        ).otherwise(0)
      )

    val evaluationWithCorrectnessDF = evaluationNonStrictDF
      .withColumn("correctAssignmentStrict",
        when(
          array_sort(col("predictedRelationshipTypes")) === array_sort(col("actualRelationshipTypes")) &&
          array_sort(col("predictedSrcLabels")) === array_sort(col("actualSrcLabels")) &&
          array_sort(col("predictedDstLabels")) === array_sort(col("actualDstLabels")),
          1
        ).otherwise(0)
      )

    val labelFrequenciesDF = explodedPredictedDF
      .join(explodedOriginalDF, Seq("edgeId"), "inner")
      .withColumn("relationshipType", explode(col("actualRelationshipTypes")))
      .groupBy("merged_cluster_id", "relationshipType")
      .agg(count("*").as("freq"))
      .withColumn("rank", row_number().over(Window.partitionBy("merged_cluster_id").orderBy(desc("freq"))))
      .filter($"rank" === 1)
      .select($"merged_cluster_id", $"relationshipType".as("majority_relationship_type"))

    val evaluationWithMajorityDF = evaluationWithCorrectnessDF
      .join(labelFrequenciesDF, Seq("merged_cluster_id"), "inner")
      .withColumn("correctAssignmentMajority",
        when(array_contains($"actualRelationshipTypes", $"majority_relationship_type"), 1)
          .otherwise(0)
      )

    val totalActualPositivesDF = explodedOriginalDF
      .groupBy(col("actualRelationshipTypes"), col("actualSrcLabels"), col("actualDstLabels"))
      .agg(count("*").as("totalActual"))


    // Majority metrics
    val TPMajority = evaluationWithMajorityDF.filter($"correctAssignmentMajority" === 1).count()
    val FPMajority = evaluationWithMajorityDF.filter($"correctAssignmentMajority" === 0).count()

    val totalPredictedPositivesMajorityDF = evaluationWithMajorityDF
      .filter($"correctAssignmentMajority" === 1)
      .groupBy(col("actualRelationshipTypes"), col("actualSrcLabels"), col("actualDstLabels"))
      .agg(count("*").as("totalPredicted"))

    val FNMajority = totalActualPositivesDF
      .join(totalPredictedPositivesMajorityDF,
        Seq("actualRelationshipTypes", "actualSrcLabels", "actualDstLabels"),
        "left_outer"
      )
      .select(
        coalesce(col("totalActual"), lit(0L)).as("totalActual"),
        coalesce(col("totalPredicted"), lit(0L)).as("totalPredicted")
      )
      .withColumn("fnPerGroup", when(col("totalActual") > col("totalPredicted"), col("totalActual") - col("totalPredicted")).otherwise(lit(0L)))
      .agg(sum(col("fnPerGroup")).as("fnCount"))
      .first()
      .getLong(0)

    val precisionMajority = if (TPMajority + FPMajority > 0) TPMajority.toDouble / (TPMajority + FPMajority) else 0.0
    val recallMajority = if (TPMajority + FNMajority > 0) TPMajority.toDouble / (TPMajority + FNMajority) else 0.0
    val f1ScoreMajority = if (precisionMajority + recallMajority > 0) 2 * (precisionMajority * recallMajority) / (precisionMajority + recallMajority) else 0.0


    println(s"\nMajority Label Edge Evaluation Metrics:")
    println(s"  True Positives (TP): $TPMajority")
    println(s"  False Positives (FP): $FPMajority")
    println(s"  False Negatives (FN): $FNMajority")
    println(f"  Precision: $precisionMajority%.4f")
    println(f"  Recall:    $recallMajority%.4f")
    println(f"  F1-Score:  $f1ScoreMajority%.4f")

   
  }
}