import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object Evaluation {

  def computeMetricsForNodes(
    spark: SparkSession,
    originalNodesDF: DataFrame, // Ground truth nodes από DataLoader
    predictedNodesDF: DataFrame // Merged patterns από LSH ή KMeans
  ): Unit = {
    import spark.implicits._

    // Explode τα predicted labels και nodeIds από το predictedNodesDF
    val explodedPredictedDF = predictedNodesDF
      .withColumn("predictedLabel", explode(col("sortedLabels")))
      .withColumn("nodeId", explode(col("nodeIdsInCluster"))) // Τώρα δίνει BIGINT
      .select(col("nodeId"), col("predictedLabel"))
      .where(col("nodeId").isNotNull)

    // Explode τα originalLabels από το originalNodesDF
    val explodedOriginalDF = originalNodesDF
      .withColumn("actualLabel", explode(col("originalLabels")))
      .select(col("_nodeId").as("nodeId"), col("actualLabel"))
      .where(col("nodeId").isNotNull)

    // Join predicted και actual labels με βάση το nodeId
    val evaluationDF = explodedPredictedDF
      .join(explodedOriginalDF, Seq("nodeId"), "inner")
      .select(col("nodeId"), col("predictedLabel"), col("actualLabel"))

    // Υπολογισμός μοναδικών labels
    val distinctGroundTruthNodes = explodedOriginalDF.select(col("actualLabel")).distinct().count()
    val distinctPredictedNodes = explodedPredictedDF.select(col("predictedLabel")).distinct().count()

    println(s"Ground Truth Nodes (distinct): $distinctGroundTruthNodes")
    println(s"Predicted Nodes (distinct):    $distinctPredictedNodes")

    // Υπολογισμός TP, FP, FN
    val clusterTypeCountsDF = evaluationDF
      .groupBy(col("predictedLabel"), col("actualLabel"))
      .agg(count("*").as("count"))

    val windowSpec = Window.partitionBy(col("predictedLabel")).orderBy(col("count").desc)

    val majorityTypeDF = clusterTypeCountsDF
      .withColumn("rank", row_number().over(windowSpec))
      .filter($"rank" === 1)
      .select(col("predictedLabel"), col("actualLabel").as("majorityType"))

    val evaluationWithMajorityDF = evaluationDF
      .join(majorityTypeDF, Seq("predictedLabel"), "left")
      .withColumn("correctAssignment",
        when(col("actualLabel") === col("majorityType"), 1).otherwise(0)
      )

    val TP = evaluationWithMajorityDF.filter($"correctAssignment" === 1).count()
    val FP = evaluationWithMajorityDF.filter($"correctAssignment" === 0).count()

    val totalActualPositivesDF = explodedOriginalDF
      .groupBy(col("actualLabel"))
      .agg(count("*").as("totalActual"))

    val totalPredictedPositivesDF = evaluationWithMajorityDF
      .groupBy(col("majorityType"))
      .agg(count("*").as("totalPredicted"))

    val FN = totalActualPositivesDF
      .join(totalPredictedPositivesDF, $"actualLabel" === $"majorityType", "left_anti")
      .agg(coalesce(sum("totalActual"), lit(0L)).as("fnCount"))
      .first()
      .getLong(0)

    val precision = if (TP + FP > 0) TP.toDouble / (TP + FP) else 0.0
    val recall = if (TP + FN > 0) TP.toDouble / (TP + FN) else 0.0
    val f1Score = if (precision + recall > 0) 2 * (precision * recall) / (precision + recall) else 0.0

    println(s"True Positives (TP): $TP")
    println(s"False Positives (FP): $FP")
    println(s"False Negatives (FN): $FN")
    println("Majority Type per Predicted Label:")
    majorityTypeDF.show()
    println("Total Actual Positives:")
    totalActualPositivesDF.show()
    println("Total Predicted Positives:")
    totalPredictedPositivesDF.show()

    println(f"\nNode Evaluation Metrics:")
    println(f"  Precision: $precision%.4f")
    println(f"  Recall:    $recall%.4f")
    println(f"  F1-Score:  $f1Score%.4f")
  }

  def computeMetricsForEdges(
    spark: SparkSession,
    originalEdgesDF: DataFrame, // Ground truth edges από DataLoader
    predictedEdgesDF: DataFrame // Merged edges από LSH ή KMeans
  ): Unit = {
    import spark.implicits._

    // Explode τα predicted relationshipTypes και edgeIds από το predictedEdgesDF
    val explodedPredictedDF = predictedEdgesDF
      .withColumn("predictedRelationshipType", explode(col("relationshipTypes")))
      .withColumn("edgeId", explode(col("edgeIdsInCluster")))
      .select(
        struct(col("edgeId.srcId").as("srcId"), col("edgeId.dstId").as("dstId")).as("edgeId"),
        col("predictedRelationshipType")
      )

    // Προετοιμασία του originalEdgesDF
    val explodedOriginalDF = originalEdgesDF
      .select(
        struct(col("srcId"), col("dstId")).as("edgeId"),
        col("relationshipType").as("actualRelationshipType"),
        col("srcType").as("actualSrcLabel"),
        col("dstType").as("actualDstLabel")
      )

    // Join predicted και actual με βάση το edgeId
    val evaluationDF = explodedPredictedDF
      .join(explodedOriginalDF, "edgeId", "inner")
      .select(
        col("edgeId"),
        col("predictedRelationshipType"),
        col("actualRelationshipType"),
        col("actualSrcLabel"),
        col("actualDstLabel")
      )

    // Υπολογισμός μοναδικών edges
    val distinctGroundTruthEdges = explodedOriginalDF
      .select(col("actualRelationshipType"), col("actualSrcLabel"), col("actualDstLabel"))
      .distinct()
      .count()
    val distinctPredictedEdges = explodedPredictedDF
      .select(col("predictedRelationshipType"))
      .distinct()
      .count()

    println(s"Ground Truth Edges (distinct): $distinctGroundTruthEdges")
    println(s"Predicted Edges (distinct):    $distinctPredictedEdges")

    // Υπολογισμός TP, FP, FN
    val clusterTypeCountsDF = evaluationDF
      .groupBy(
        col("predictedRelationshipType"),
        col("actualRelationshipType"),
        col("actualSrcLabel"),
        col("actualDstLabel")
      )
      .agg(count("*").as("count"))

    val windowSpec = Window.partitionBy(col("predictedRelationshipType")).orderBy(col("count").desc)

    val majorityTypeDF = clusterTypeCountsDF
      .withColumn("rank", row_number().over(windowSpec))
      .filter($"rank" === 1)
      .select(
        col("predictedRelationshipType"),
        col("actualRelationshipType").as("majorityRelationshipType"),
        col("actualSrcLabel").as("majoritySrcLabel"),
        col("actualDstLabel").as("majorityDstLabel")
      )

    val evaluationWithMajorityDF = evaluationDF
      .join(majorityTypeDF, Seq("predictedRelationshipType"), "left")
      .withColumn("correctAssignment",
        when(
          $"actualRelationshipType" === $"majorityRelationshipType" &&
          $"actualSrcLabel" === $"majoritySrcLabel" &&
          $"actualDstLabel" === $"majorityDstLabel",
          1
        ).otherwise(0)
      )

    val TP = evaluationWithMajorityDF.filter($"correctAssignment" === 1).count()
    val FP = evaluationWithMajorityDF.filter($"correctAssignment" === 0).count()

    val totalActualPositivesDF = explodedOriginalDF
      .groupBy(col("actualRelationshipType"), col("actualSrcLabel"), col("actualDstLabel"))
      .agg(count("*").as("totalActual"))

    val totalPredictedPositivesDF = evaluationWithMajorityDF
      .groupBy(col("majorityRelationshipType"), col("majoritySrcLabel"), col("majorityDstLabel"))
      .agg(count("*").as("totalPredicted"))

    val FN = totalActualPositivesDF
      .join(
        totalPredictedPositivesDF,
        $"actualRelationshipType" === $"majorityRelationshipType" &&
        $"actualSrcLabel" === $"majoritySrcLabel" &&
        $"actualDstLabel" === $"majorityDstLabel",
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
    println("Majority Type per Predicted Relationship Type:")
    majorityTypeDF.show()
    println("Total Actual Positives:")
    totalActualPositivesDF.show()
    println("Total Predicted Positives:")
    totalPredictedPositivesDF.show()

    println(f"\nEdge Evaluation Metrics:")
    println(f"  Precision: $precision%.4f")
    println(f"  Recall:    $recall%.4f")
    println(f"  F1-Score:  $f1Score%.4f")
  }
}