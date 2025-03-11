import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object Evaluation {

  def computeIncrementalMetricsForNodes(
    evaluationDF: DataFrame,
    entityCol: String,
    predictedCol: String,
    originalCol: String,
    batchIndex: Int
  ): Unit = {
    val spark = evaluationDF.sparkSession
    import spark.implicits._

    val explodedDF = evaluationDF
      .withColumn("label", explode(col(originalCol)))
      .withColumn("actualLabel", 
        when(col("label").isNotNull, col("label"))
        .otherwise(array(lit("Unknown")))
      )

    val clusterTypeCountsDF = explodedDF.groupBy(predictedCol, "actualLabel").count()

    val windowSpec = Window.partitionBy(predictedCol).orderBy(col("count").desc)

    val majorityTypeDF = clusterTypeCountsDF
      .withColumn("rank", row_number().over(windowSpec))
      .filter($"rank" === 1)
      .select(col(predictedCol), col("actualLabel").as("majorityType"))

    val evaluationWithMajorityDF = explodedDF
      .join(majorityTypeDF, Seq(predictedCol), "left")
      .withColumn("correctAssignment",
        when(col("actualLabel") === col("majorityType"), 1).otherwise(0)
      )

    val TP = evaluationWithMajorityDF.filter(col("correctAssignment") === 1).count()
    val FP = evaluationWithMajorityDF.filter(col("correctAssignment") === 0).count()

    val totalActualPositivesDF = explodedDF
      .groupBy("actualLabel")
      .count()
      .withColumnRenamed("count", "totalActual")

    val totalPredictedPositivesDF = evaluationWithMajorityDF
      .groupBy("majorityType")
      .count()
      .withColumnRenamed("count", "totalPredicted")

    val FN = totalActualPositivesDF
      .join(totalPredictedPositivesDF, 
        totalActualPositivesDF("actualLabel") === totalPredictedPositivesDF("majorityType"),
        "left_anti"
      )
      .agg(coalesce(sum("totalActual"), lit(0)))
      .collect()
      .headOption
      .map(_.getLong(0))
      .getOrElse(0L)

    val precision = if (TP + FP > 0) TP.toDouble / (TP + FP) else 0.0
    val recall = if (TP + FN > 0) TP.toDouble / (TP + FN) else 0.0
    val f1Score = if (precision + recall > 0) 2 * (precision * recall) / (precision + recall) else 0.0

    println(s"False Negatives (FN): $FN")
    majorityTypeDF.show()
    totalActualPositivesDF.show()
    totalPredictedPositivesDF.show()

    println(f"\nBatch #$batchIndex Node Evaluation Metrics:")
    println(f"  Precision: $precision%.4f")
    println(f"  Recall:    $recall%.4f")
    println(f"  F1-Score:  $f1Score%.4f")
  }

  def computeIncrementalMetricsForEdges(
    evaluationDF: DataFrame, 
    predictedCol: String, 
    relationshipTypeCol: String, 
    srcLabelCol: String, 
    dstLabelCol: String, 
    propsCol: String, 
    batchIndex: Int
  ): Unit = {
    val spark = evaluationDF.sparkSession
    import spark.implicits._
    import org.apache.spark.sql.expressions.Window

    val explodedDF = evaluationDF
      .withColumn(srcLabelCol, explode(col(srcLabelCol)))
      .withColumn(dstLabelCol, explode(col(dstLabelCol)))
      .withColumn(propsCol, explode(col(propsCol)))

    val clusterTypeCountsDF = explodedDF
      .groupBy(predictedCol, relationshipTypeCol, srcLabelCol, dstLabelCol, propsCol)
      .count()

    val windowSpec = Window.partitionBy(predictedCol).orderBy(col("count").desc)

    val majorityTypeDF = clusterTypeCountsDF
      .withColumn("rank", row_number().over(windowSpec))
      .filter($"rank" === 1)
      .select(
        col(predictedCol),
        col(relationshipTypeCol).as("majorityRelationshipType"),
        col(srcLabelCol).as("majoritySrcLabel"),
        col(dstLabelCol).as("majorityDstLabel"),
        col(propsCol).as("majorityProperties")
      )

    val evaluationWithMajorityDF = explodedDF
      .join(majorityTypeDF, Seq(predictedCol), "left")
      .withColumn("correctAssignment",
        when(
          col(relationshipTypeCol) === col("majorityRelationshipType") &&
          col(srcLabelCol) === col("majoritySrcLabel") &&
          col(dstLabelCol) === col("majorityDstLabel") &&
          col(propsCol) === col("majorityProperties"),
          1
        ).otherwise(0)
      )

    val TP = evaluationWithMajorityDF.filter(col("correctAssignment") === 1).count()
    val FP = evaluationWithMajorityDF.filter(col("correctAssignment") === 0).count()

    val totalActualPositivesDF = explodedDF
      .groupBy(relationshipTypeCol, srcLabelCol, dstLabelCol, propsCol)
      .count()
      .withColumnRenamed("count", "totalActual")

    val totalPredictedPositivesDF = evaluationWithMajorityDF
      .groupBy("majorityRelationshipType", "majoritySrcLabel", "majorityDstLabel", "majorityProperties")
      .count()
      .withColumnRenamed("count", "totalPredicted")

    val FN = totalActualPositivesDF
      .join(totalPredictedPositivesDF,
        totalActualPositivesDF(relationshipTypeCol) === totalPredictedPositivesDF("majorityRelationshipType") &&
        totalActualPositivesDF(srcLabelCol) === totalPredictedPositivesDF("majoritySrcLabel") &&
        totalActualPositivesDF(dstLabelCol) === totalPredictedPositivesDF("majorityDstLabel") &&
        totalActualPositivesDF(propsCol) === totalPredictedPositivesDF("majorityProperties"),
        "left_anti"
      )
      .agg(coalesce(sum("totalActual"), lit(0)))
      .collect()
      .headOption
      .map(_.getLong(0))
      .getOrElse(0L)

    val precision = if (TP + FP > 0) TP.toDouble / (TP + FP) else 0.0
    val recall = if (TP + FN > 0) TP.toDouble / (TP + FN) else 0.0
    val f1Score = if (precision + recall > 0) 2 * (precision * recall) / (precision + recall) else 0.0

    println(f"\nBatch #$batchIndex Edge Evaluation Metrics:")
    println(f"  Precision: $precision%.4f")
    println(f"  Recall:    $recall%.4f")
    println(f"  F1-Score:  $f1Score%.4f")
  }
}
