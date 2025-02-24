import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object Evaluation {

  def computeMetricsWithoutPairwise(evaluationDF: DataFrame, entityCol: String = "EntityType"): Unit = {
    val spark = evaluationDF.sparkSession
    import spark.implicits._

    // Flatten the collected labels
    val explodedDF = evaluationDF
      .withColumn("actualLabel", explode(col(entityCol))) // Flatten the entity labels
      .filter($"actualLabel".isNotNull) // Remove nulls

    // Compute majority type for each cluster (hashes)
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

    // Compute True Positives (TP) and False Positives (FP)
    val TP = evaluationWithMajorityDF
      .filter($"actualLabel" === $"majorityType")
      .count()

    val FP = evaluationWithMajorityDF
      .filter($"actualLabel" =!= $"majorityType")
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

    println(f"$entityCol Evaluation Metrics:")
    println(f"Precision: $precision%.4f")
    println(f"Recall: $recall%.4f")
    println(f"F1-Score: $f1Score%.4f")
  }


  def computeMetricsWithoutPairwiseJaccard(evaluationDF: DataFrame, entityCol: String, predictedCol: String): Unit = {
    val spark = evaluationDF.sparkSession
    import spark.implicits._

    val explodedDF = evaluationDF
      .withColumn("actualLabel", explode(col(entityCol)))
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

    val TP = evaluationWithMajorityDF
      .filter($"actualLabel" === $"majorityType")
      .count()

    val FP = evaluationWithMajorityDF
      .filter($"actualLabel" =!= $"majorityType")
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

    println(f"$entityCol Evaluation Metrics:")
    println(f"Precision: $precision%.4f")
    println(f"Recall: $recall%.4f")
    println(f"F1-Score: $f1Score%.4f")
  }
}
