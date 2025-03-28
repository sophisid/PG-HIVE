import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._

object Evaluation {

  def computeMetricsForNodes(
    spark: SparkSession,
    originalNodesDF: DataFrame,
    predictedNodesDF: DataFrame
  ): Unit = {
    import spark.implicits._

    val explodedPredictedDF = predictedNodesDF
      .withColumn("nodeId", explode(col("nodeIdsInCluster")))
      .withColumn("predictedLabels", split(concat_ws(":", $"sortedLabels"), ":"))
      .select(col("nodeId"), col("predictedLabels"), col("merged_cluster_id"))
      .where(col("nodeId").isNotNull)

    val explodedOriginalDF = originalNodesDF
      .withColumn("actualLabel", explode(col("originalLabels")))
      .select(col("_nodeId").as("nodeId"), col("actualLabel"))
      .where(col("nodeId").isNotNull)

    val evaluationDF = explodedPredictedDF
      .join(explodedOriginalDF, Seq("nodeId"), "inner")
      .select(col("nodeId"), col("predictedLabels"), col("actualLabel"), col("merged_cluster_id"))

    val distinctGroundTruthNodes = explodedOriginalDF.select(col("actualLabel")).distinct().count()
    val distinctPredictedNodes = predictedNodesDF.select(col("merged_cluster_id")).distinct().count()

    println(s"Ground Truth Nodes (distinct labels): $distinctGroundTruthNodes")
    println(s"Predicted Nodes (distinct clusters): $distinctPredictedNodes")

    val evaluationWithCorrectnessDF = evaluationDF
      .withColumn("correctAssignment",
        when(array_contains(col("predictedLabels"), col("actualLabel")), 1).otherwise(0)
      )

    val TP = evaluationWithCorrectnessDF.filter($"correctAssignment" === 1).count()
    val FP = evaluationWithCorrectnessDF.filter($"correctAssignment" === 0).count()

    val totalActualPositivesDF = explodedOriginalDF
      .groupBy(col("actualLabel"))
      .agg(count("*").as("totalActual"))
      .withColumnRenamed("actualLabel", "actualLabelActual")

    val totalPredictedPositivesDF = evaluationWithCorrectnessDF
      .filter($"correctAssignment" === 1)
      .groupBy(col("actualLabel"))
      .agg(count("*").as("totalPredicted"))
      .withColumnRenamed("actualLabel", "actualLabelPredicted")

    val FN = totalActualPositivesDF
      .join(totalPredictedPositivesDF,
            totalActualPositivesDF("actualLabelActual") === totalPredictedPositivesDF("actualLabelPredicted"),
            "left_anti")
      .agg(coalesce(sum("totalActual"), lit(0L)).as("fnCount"))
      .first()
      .getLong(0)

    val precision = if (TP + FP > 0) TP.toDouble / (TP + FP) else 0.0
    val recall = if (TP + FN > 0) TP.toDouble / (TP + FN) else 0.0
    val f1Score = if (precision + recall > 0) 2 * (precision * recall) / (precision + recall) else 0.0

    println(s"True Positives (TP): $TP")
    println(s"False Positives (FP): $FP")
    println(s"False Negatives (FN): $FN")
    println("Evaluation Sample with Cluster IDs:")
    evaluationWithCorrectnessDF.show(10, false)
    println("Total Actual Positives:")
    totalActualPositivesDF.show(false)
    println("Total Predicted Positives:")
    totalPredictedPositivesDF.show(false)

    println(f"\nNode Evaluation Metrics:")
    println(f"  Precision: $precision%.4f")
    println(f"  Recall:    $recall%.4f")
    println(f"  F1-Score:  $f1Score%.4f")
  }

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
        col("relationshipTypes").as("predictedRelationshipTypes"),
        col("srcLabels").as("predictedSrcLabels"),
        col("dstLabels").as("predictedDstLabels"),
        col("merged_cluster_id")
      )

    val explodedOriginalDF = originalEdgesDF
      .select(
        struct(col("srcId"), col("dstId")).as("edgeId"),
        col("relationshipType").as("actualRelationshipType"),
        col("srcType").as("actualSrcLabel"),
        col("dstType").as("actualDstLabel")
      )

    val evaluationDF = explodedPredictedDF
      .join(explodedOriginalDF, "edgeId", "inner")
      .select(
        col("edgeId"),
        col("predictedRelationshipTypes"),
        col("predictedSrcLabels"),
        col("predictedDstLabels"),
        col("actualRelationshipType"),
        col("actualSrcLabel"),
        col("actualDstLabel"),
        col("merged_cluster_id")
      )

    val distinctGroundTruthEdges = explodedOriginalDF
      .select(col("actualRelationshipType"), col("actualSrcLabel"), col("actualDstLabel"))
      .distinct()
      .count()
    val distinctPredictedEdges = predictedEdgesDF
      .select(col("merged_cluster_id"))
      .distinct()
      .count()

    println(s"Ground Truth Edges (distinct): $distinctGroundTruthEdges")
    println(s"Predicted Edges (distinct clusters): $distinctPredictedEdges")

    val evaluationWithCorrectnessDF = evaluationDF
      .withColumn("correctAssignment",
        when(
          array_contains(col("predictedRelationshipTypes"), col("actualRelationshipType")) &&
          array_contains(col("predictedSrcLabels"), col("actualSrcLabel")) &&
          array_contains(col("predictedDstLabels"), col("actualDstLabel")),
          1
        ).otherwise(0)
      )

    val TP = evaluationWithCorrectnessDF.filter($"correctAssignment" === 1).count()
    val FP = evaluationWithCorrectnessDF.filter($"correctAssignment" === 0).count()

    val totalActualPositivesDF = explodedOriginalDF
      .groupBy(col("actualRelationshipType"), col("actualSrcLabel"), col("actualDstLabel"))
      .agg(count("*").as("totalActual"))

    val totalPredictedPositivesDF = evaluationWithCorrectnessDF
      .filter($"correctAssignment" === 1)
      .groupBy(col("actualRelationshipType"), col("actualSrcLabel"), col("actualDstLabel"))
      .agg(count("*").as("totalPredicted"))

    val FN = totalActualPositivesDF
      .join(totalPredictedPositivesDF,
        totalActualPositivesDF("actualRelationshipType") === totalPredictedPositivesDF("actualRelationshipType") &&
        totalActualPositivesDF("actualSrcLabel") === totalPredictedPositivesDF("actualSrcLabel") &&
        totalActualPositivesDF("actualDstLabel") === totalPredictedPositivesDF("actualDstLabel"),
        "left_anti"
      )
      .agg(coalesce(sum("totalActual"), lit(0L)).as("fnCount"))
      .first()
      .getLong(0)

    val precision = if (TP + FP > 0) TP.toDouble / (TP + FP) else 0.0
    val recall = if (TP + FN > 0) TP.toDouble / (TP + FN) else 0.0
    val f1Score = if (precision + recall > 0) 2 * (precision * recall) / (precision + recall) else 0.0

    println(s"True Positives (TP): $TP")
    println(s"False Positives (FP): $FP")
    println(s"False Negatives (FN): $FN")
    println("Evaluation Sample with Cluster IDs:")
    evaluationWithCorrectnessDF.show(10, false)
    println("Total Actual Positives:")
    totalActualPositivesDF.show(false)
    println("Total Predicted Positives:")
    totalPredictedPositivesDF.show(false)

    println(f"\nEdge Evaluation Metrics:")
    println(f"  Precision: $precision%.4f")
    println(f"  Recall:    $recall%.4f")
    println(f"  F1-Score:  $f1Score%.4f")
  }
}