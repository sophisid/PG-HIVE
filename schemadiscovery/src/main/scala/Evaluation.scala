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

    // Explode predicted nodes to get nodeId and predicted labels
    val explodedPredictedDF = predictedNodesDF
      .withColumn("nodeId", explode(col("nodeIdsInCluster")))
      .withColumn("predictedLabels", array_distinct(split(concat_ws(":", $"sortedLabels"), ":")))
      .select(col("nodeId"), col("predictedLabels"), col("merged_cluster_id"))
      .where(col("nodeId").isNotNull)

    // Explode original nodes to get nodeId and actual labels
    val explodedOriginalDF = originalNodesDF
      .withColumn("actualLabels",
        when(col("original_label").isNotNull, split(col("original_label"), ","))
          .otherwise(array().cast("array<string>")))
      .select(col("_nodeId").as("nodeId"), col("actualLabels"))
      .where(col("nodeId").isNotNull)

    // Join predicted and original for evaluation
    val evaluationDF = explodedPredictedDF
      .join(explodedOriginalDF, Seq("nodeId"), "inner")
      .select(col("nodeId"), col("predictedLabels"), col("actualLabels"), col("merged_cluster_id"))

    // Compute distinct ground truth and predicted clusters
    val distinctGroundTruthNodes = explodedOriginalDF.select(col("actualLabels")).distinct().count()
    val distinctPredictedNodes = predictedNodesDF.select(col("merged_cluster_id")).distinct().count()

    println(s"Ground Truth Nodes (distinct label sets): $distinctGroundTruthNodes")
    println(s"Predicted Nodes (distinct clusters): $distinctPredictedNodes")

    // Non-Strict Evaluation
    val evaluationNonStrictDF = evaluationDF
      .withColumn("correctAssignmentNonStrict",
        when(size(array_intersect(col("actualLabels"), col("predictedLabels"))) > 0, 1)
          .otherwise(0)
      )

    // Strict Evaluation
    val evaluationWithCorrectnessDF = evaluationNonStrictDF
      .withColumn("correctAssignmentStrict",
        when(array_sort(col("actualLabels")) === array_sort(col("predictedLabels")), 1)
          .otherwise(0)
      )

    // --- Majority Label Evaluation ---
    // Step 1: Compute label frequencies per cluster
    val labelFrequenciesDF = predictedNodesDF
      .withColumn("label", explode(col("sortedLabels")))
      .groupBy("merged_cluster_id", "label")
      .agg(count("*").as("freq"))
      .withColumn("rank", row_number().over(Window.partitionBy("merged_cluster_id").orderBy(desc("freq"))))
      .filter($"rank" === 1)
      .select($"merged_cluster_id", $"label".as("majority_label"))

    // Step 2: Join majority label with evaluation DF and check correctness
    val evaluationWithMajorityDF = evaluationWithCorrectnessDF
      .join(labelFrequenciesDF, Seq("merged_cluster_id"), "inner")
      .withColumn("correctAssignmentMajority",
        when(array_contains($"actualLabels", $"majority_label"), 1)
          .otherwise(0)
      )

    // --- Compute Metrics ---
    // Non-Strict Metrics
    val TPNonStrict = evaluationWithMajorityDF.filter($"correctAssignmentNonStrict" === 1).count()
    val FPNonStrict = evaluationWithMajorityDF.filter($"correctAssignmentNonStrict" === 0).count()

    val totalActualPositivesDF = explodedOriginalDF
      .groupBy(col("actualLabels"))
      .agg(count("*").as("totalActual"))

    val totalPredictedPositivesNonStrictDF = evaluationWithMajorityDF
      .filter($"correctAssignmentNonStrict" === 1)
      .groupBy(col("actualLabels"))
      .agg(count("*").as("totalPredicted"))

    val FNNonStrict = totalActualPositivesDF
      .join(totalPredictedPositivesNonStrictDF, Seq("actualLabels"), "left_outer")
      .select(
        coalesce(col("totalActual"), lit(0L)).as("totalActual"),
        coalesce(col("totalPredicted"), lit(0L)).as("totalPredicted")
      )
      .withColumn("fnPerGroup", when(col("totalActual") > col("totalPredicted"), col("totalActual") - col("totalPredicted")).otherwise(lit(0L)))
      .agg(sum(col("fnPerGroup")).as("fnCount"))
      .first()
      .getLong(0)

    val precisionNonStrict = if (TPNonStrict + FPNonStrict > 0) TPNonStrict.toDouble / (TPNonStrict + FPNonStrict) else 0.0
    val recallNonStrict = if (TPNonStrict + FNNonStrict > 0) TPNonStrict.toDouble / (TPNonStrict + FNNonStrict) else 0.0
    val f1ScoreNonStrict = if (precisionNonStrict + recallNonStrict > 0) 2 * (precisionNonStrict * recallNonStrict) / (precisionNonStrict + recallNonStrict) else 0.0

    // Strict Metrics
    val TPStrict = evaluationWithMajorityDF.filter($"correctAssignmentStrict" === 1).count()
    val FPStrict = evaluationWithMajorityDF.filter($"correctAssignmentStrict" === 0).count()

    val totalPredictedPositivesStrictDF = evaluationWithMajorityDF
      .filter($"correctAssignmentStrict" === 1)
      .groupBy(col("actualLabels"))
      .agg(count("*").as("totalPredicted"))

    val FNStrict = totalActualPositivesDF
      .join(totalPredictedPositivesStrictDF, Seq("actualLabels"), "left_outer")
      .select(
        coalesce(col("totalActual"), lit(0L)).as("totalActual"),
        coalesce(col("totalPredicted"), lit(0L)).as("totalPredicted")
      )
      .withColumn("fnPerGroup", when(col("totalActual") > col("totalPredicted"), col("totalActual") - col("totalPredicted")).otherwise(lit(0L)))
      .agg(sum(col("fnPerGroup")).as("fnCount"))
      .first()
      .getLong(0)

    val precisionStrict = if (TPStrict + FPStrict > 0) TPStrict.toDouble / (TPStrict + FPStrict) else 0.0
    val recallStrict = if (TPStrict + FNStrict > 0) TPStrict.toDouble / (TPStrict + FNStrict) else 0.0
    val f1ScoreStrict = if (precisionStrict + recallStrict > 0) 2 * (precisionStrict * recallStrict) / (precisionStrict + recallStrict) else 0.0

    // Majority Label Metrics
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

    // --- Print Results ---
    println(s"\nNon-Strict Node Evaluation Metrics:")
    println(s"  True Positives (TP): $TPNonStrict")
    println(s"  False Positives (FP): $FPNonStrict")
    println(s"  False Negatives (FN): $FNNonStrict")
    println(f"  Precision: $precisionNonStrict%.4f")
    println(f"  Recall:    $recallNonStrict%.4f")
    println(f"  F1-Score:  $f1ScoreNonStrict%.4f")

    println(s"\nStrict Node Evaluation Metrics:")
    println(s"  True Positives (TP): $TPStrict")
    println(s"  False Positives (FP): $FPStrict")
    println(s"  False Negatives (FN): $FNStrict")
    println(f"  Precision: $precisionStrict%.4f")
    println(f"  Recall:    $recallStrict%.4f")
    println(f"  F1-Score:  $f1ScoreStrict%.4f")

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

    println("Total Actual Positives:")
    totalActualPositivesDF.show(false)
    println("Total Predicted Positives (Non-Strict):")
    totalPredictedPositivesNonStrictDF.show(false)
    println("Total Predicted Positives (Strict):")
    totalPredictedPositivesStrictDF.show(false)
    println("Total Predicted Positives (Majority):")
    totalPredictedPositivesMajorityDF.show(false)
  }

  // Η computeMetricsForEdges παραμένει ως έχει
  def computeMetricsForEdges(
    spark: SparkSession,
    originalEdgesDF: DataFrame,
    predictedEdgesDF: DataFrame
  ): Unit = {
    import spark.implicits._

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

    val TPNonStrict = evaluationWithCorrectnessDF.filter($"correctAssignmentNonStrict" === 1).count()
    val FPNonStrict = evaluationWithCorrectnessDF.filter($"correctAssignmentNonStrict" === 0).count()

    val totalActualPositivesDF = explodedOriginalDF
      .groupBy(col("actualRelationshipTypes"), col("actualSrcLabels"), col("actualDstLabels"))
      .agg(count("*").as("totalActual"))

    val totalPredictedPositivesNonStrictDF = evaluationWithCorrectnessDF
      .filter($"correctAssignmentNonStrict" === 1)
      .groupBy(col("actualRelationshipTypes"), col("actualSrcLabels"), col("actualDstLabels"))
      .agg(count("*").as("totalPredicted"))

    val FNNonStrict = totalActualPositivesDF
      .join(totalPredictedPositivesNonStrictDF,
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

    val precisionNonStrict = if (TPNonStrict + FPNonStrict > 0) TPNonStrict.toDouble / (TPNonStrict + FPNonStrict) else 0.0
    val recallNonStrict = if (TPNonStrict + FNNonStrict > 0) TPNonStrict.toDouble / (TPNonStrict + FNNonStrict) else 0.0
    val f1ScoreNonStrict = if (precisionNonStrict + recallNonStrict > 0) 2 * (precisionNonStrict * recallNonStrict) / (precisionNonStrict + recallNonStrict) else 0.0

    val TPStrict = evaluationWithCorrectnessDF.filter($"correctAssignmentStrict" === 1).count()
    val FPStrict = evaluationWithCorrectnessDF.filter($"correctAssignmentStrict" === 0).count()

    val totalPredictedPositivesStrictDF = evaluationWithCorrectnessDF
      .filter($"correctAssignmentStrict" === 1)
      .groupBy(col("actualRelationshipTypes"), col("actualSrcLabels"), col("actualDstLabels"))
      .agg(count("*").as("totalPredicted"))

    val FNStrict = totalActualPositivesDF
      .join(totalPredictedPositivesStrictDF,
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

    val precisionStrict = if (TPStrict + FPStrict > 0) TPStrict.toDouble / (TPStrict + FPStrict) else 0.0
    val recallStrict = if (TPStrict + FNStrict > 0) TPStrict.toDouble / (TPStrict + FNStrict) else 0.0
    val f1ScoreStrict = if (precisionStrict + recallStrict > 0) 2 * (precisionStrict * recallStrict) / (precisionStrict + recallStrict) else 0.0

    println(s"\nNon-Strict Edge Evaluation Metrics:")
    println(s"  True Positives (TP): $TPNonStrict")
    println(s"  False Positives (FP): $FPNonStrict")
    println(s"  False Negatives (FN): $FNNonStrict")
    println(f"  Precision: $precisionNonStrict%.4f")
    println(f"  Recall:    $recallNonStrict%.4f")
    println(f"  F1-Score:  $f1ScoreNonStrict%.4f")

    println(s"\nStrict Edge Evaluation Metrics:")
    println(s"  True Positives (TP): $TPStrict")
    println(s"  False Positives (FP): $FPStrict")
    println(s"  False Negatives (FN): $FNStrict")
    println(f"  Precision: $precisionStrict%.4f")
    println(f"  Recall:    $recallStrict%.4f")
    println(f"  F1-Score:  $f1ScoreStrict%.4f")

    println("\nEvaluation Sample with Cluster IDs (Strict and Non-Strict):")
    evaluationWithCorrectnessDF.show(10, false)
    println("Total Actual Positives:")
    totalActualPositivesDF.show(false)
    println("Total Predicted Positives (Non-Strict):")
    totalPredictedPositivesNonStrictDF.show(false)
    println("Total Predicted Positives (Strict):")
    totalPredictedPositivesStrictDF.show(false)
  }
}