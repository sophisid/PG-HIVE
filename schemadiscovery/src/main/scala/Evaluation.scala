import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object Evaluation {

  def computeMetricsWithOriginalLabels(evaluationDF: DataFrame, entityCol: String, originalCol: String): Unit = {
    val spark = evaluationDF.sparkSession
    import spark.implicits._

    // Flatten labels, including original labels if needed
    val explodedDF = if (evaluationDF.columns.contains("originalLabels")) {
      evaluationDF.withColumn("actualLabel", explode(
        when(size(col(entityCol)) > 0, col(entityCol)).otherwise(col("originalLabels"))
      ))
    } else {
      evaluationDF.withColumn("actualLabel", explode(col(entityCol))) // Αν δεν υπάρχει originalLabels, αγνόησέ το
    }


    // Compute majority type for each cluster
    val clusterTypeCountsDF = explodedDF
      .groupBy("hashes", "actualLabel")
      .count()

    val windowSpec = Window.partitionBy("hashes").orderBy(col("count").desc)

    val majorityTypeDF = clusterTypeCountsDF
      .withColumn("rank", row_number().over(windowSpec))
      .filter($"rank" === 1)
      .select($"hashes", $"actualLabel".as("majorityType"))

    // Attach majority type to each node
    val evaluationWithMajorityDF = explodedDF
      .join(majorityTypeDF, "hashes")
      .withColumn("correctAssignment",
        when($"actualLabel" === $"majorityType", 1).otherwise(0)
      )

    val TP = evaluationWithMajorityDF
      .filter($"correctAssignment" === 1)
      .count()

    val FP = evaluationWithMajorityDF
      .filter($"correctAssignment" === 0)
      .count()

    // Compute False Negatives (FN)
    val totalActualPositivesDF = explodedDF
      .groupBy("actualLabel")
      .count()
      .withColumnRenamed("count", "totalActual")

    val totalPredictedPositivesDF = evaluationWithMajorityDF
      .groupBy("majorityType")
      .count()
      .withColumnRenamed("count", "totalPredicted")

    val FN = totalActualPositivesDF.join(
      totalPredictedPositivesDF,
      totalActualPositivesDF("actualLabel") === totalPredictedPositivesDF("majorityType"),
      "left_outer"
    )
      .withColumn("FN", $"totalActual" - coalesce($"totalPredicted", lit(0)))
      .agg(sum("FN"))
      .collect()(0).getLong(0)

    // Compute Precision, Recall, and F1-Score
    val precision = if (TP + FP > 0) TP.toDouble / (TP + FP) else 0.0
    val recall = if (TP + FN > 0) TP.toDouble / (TP + FN) else 0.0
    val f1Score = if (precision + recall > 0) 2 * (precision * recall) / (precision + recall) else 0.0

    println(f"\n$entityCol Evaluation Metrics:")
    println(f"  Precision: $precision%.4f")
    println(f"  Recall:    $recall%.4f")
    println(f"  F1-Score:  $f1Score%.4f")
  }

  def computeMetricsWithOriginalLabelsJaccard(evaluationDF: DataFrame, entityCol: String, predictedCol: String, originalCol: String): Unit = {
    val spark = evaluationDF.sparkSession
    import spark.implicits._

    val explodedDF = evaluationDF
      .withColumn("actualLabel", explode(
        when(size(col(entityCol)) > 0, col(entityCol))
          .otherwise(col(originalCol))
      ))
      .filter($"actualLabel".isNotNull)

    val clusterTypeCountsDF = explodedDF
      .groupBy(predictedCol, "actualLabel")
      .count()

    val windowSpec = Window.partitionBy(predictedCol).orderBy(col("count").desc)

    val majorityTypeDF = clusterTypeCountsDF
      .withColumn("rank", row_number().over(windowSpec))
      .filter($"rank" === 1)
      .select(col(predictedCol), $"actualLabel".as("majorityType"))

    val evaluationWithMajorityDF = explodedDF
      .join(majorityTypeDF, predictedCol)
      .withColumn("correctAssignment",
        when($"actualLabel" === $"majorityType", 1).otherwise(0)
      )

    val TP = evaluationWithMajorityDF
      .filter($"correctAssignment" === 1)
      .count()

    val FP = evaluationWithMajorityDF
      .filter($"correctAssignment" === 0)
      .count()

    val totalActualPositivesDF = explodedDF
      .groupBy("actualLabel")
      .count()
      .withColumnRenamed("count", "totalActual")

    val totalPredictedPositivesDF = evaluationWithMajorityDF
      .groupBy("majorityType")
      .count()
      .withColumnRenamed("count", "totalPredicted")

    val FN = totalActualPositivesDF.join(
      totalPredictedPositivesDF,
      totalActualPositivesDF("actualLabel") === totalPredictedPositivesDF("majorityType"),
      "left_outer"
    )
      .withColumn("FN", $"totalActual" - coalesce($"totalPredicted", lit(0)))
      .agg(sum("FN"))
      .collect()(0).getLong(0)

    val precision = if (TP + FP > 0) TP.toDouble / (TP + FP) else 0.0
    val recall = if (TP + FN > 0) TP.toDouble / (TP + FN) else 0.0
    val f1Score = if (precision + recall > 0) 2 * (precision * recall) / (precision + recall) else 0.0

    println(f"\n$entityCol (Jaccard) Evaluation Metrics:")
    println(f"  Precision: $precision%.4f")
    println(f"  Recall:    $recall%.4f")
    println(f"  F1-Score:  $f1Score%.4f")
  }
}
