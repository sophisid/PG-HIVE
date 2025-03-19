import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object GroupedEvaluation {

  // Παράδειγμα για grouped nodes:
  // groupedNodesDF: (label, nonOptionalProps, optionalProps)
  // nodeAssignmentsDF: (nodeId, predictedGroupedLabel)
  // groundTruthDF: (nodeId, actualLabel)
  def computeGroupedNodeMetrics(
    groupedNodesDF: DataFrame,
    nodeAssignmentsDF: DataFrame,
    groundTruthDF: DataFrame
  ): Unit = {
    val spark = groupedNodesDF.sparkSession
    import spark.implicits._

    // (nodeId, predictedGroupedLabel) + (nodeId, actualLabel)
    val joinedDF = nodeAssignmentsDF.join(groundTruthDF, "nodeId")

    // Majority vote (αν χρειάζεται) ή άμεση χρήση predictedGroupedLabel
    val dfWithCorrect = joinedDF.withColumn("correct", when($"predictedGroupedLabel" === $"actualLabel", 1).otherwise(0))

    val TP = dfWithCorrect.filter($"correct" === 1).count()
    val total = dfWithCorrect.count()
    val FP = total - TP

    // Παράδειγμα: FN = κόμβοι εκτός grouping ή που δεν έπιασε το σωστό label
    // Προσαρμογή ανάλογα με τη λογική σου
    val FN = 0L // ή υπολόγισέ το με πρόσθετο join αν χρειάζεται

    val precision = if (TP + FP > 0) TP.toDouble / (TP + FP) else 0.0
    val recall    = if (TP + FN > 0) TP.toDouble / (TP + FN) else 0.0
    val f1        = if (precision + recall > 0) 2 * (precision * recall) / (precision + recall) else 0.0

    println(s"Grouped Node Patterns: Precision=$precision, Recall=$recall, F1=$f1")
  }

  // Παράδειγμα για grouped edges:
  // groupedEdgesDF: (relationshipTypes, srcLabels, dstLabels, nonOptionalProps, optionalProps)
  // edgeAssignmentsDF: (edgeId, predictedGroupKey)
  // groundTruthDF: (edgeId, actualRelType, actualSrcLabel, actualDstLabel, ...)
  def computeGroupedEdgeMetrics(
    groupedEdgesDF: DataFrame,
    edgeAssignmentsDF: DataFrame,
    groundTruthDF: DataFrame
  ): Unit = {
    val spark = groupedEdgesDF.sparkSession
    import spark.implicits._

    val joinedDF = edgeAssignmentsDF.join(groundTruthDF, "edgeId")
    val dfWithCorrect = joinedDF.withColumn(
      "correct",
      when(
        $"predictedGroupKey" === $"actualRelType", // ή άλλη λογική σύγκρισης (src/dst κτλ.)
        1
      ).otherwise(0)
    )

    val TP = dfWithCorrect.filter($"correct" === 1).count()
    val total = dfWithCorrect.count()
    val FP = total - TP
    val FN = 0L // ή αναλόγως

    val precision = if (TP + FP > 0) TP.toDouble / (TP + FP) else 0.0
    val recall    = if (TP + FN > 0) TP.toDouble / (TP + FN) else 0.0
    val f1        = if (precision + recall > 0) 2 * (precision * recall) / (precision + recall) else 0.0

    println(s"Grouped Edge Patterns: Precision=$precision, Recall=$recall, F1=$f1")
  }
}
