import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Preprocessing {
  def preprocessing(spark: SparkSession, nodesDF: DataFrame, EdgesDF: DataFrame, dropProbability: Double): (DataFrame, DataFrame) = {
    import spark.implicits._

    // Create the knownLabels column
    val nodesWithKnown = nodesDF.withColumn("knownLabels", split(col("_labels"), ":"))
    println("Nodes with knownLabels:")
    nodesWithKnown.select("_nodeId", "knownLabels").show(10, truncate = false)

    // Remove knownLabels with a certain probability
    val nodesAfterRemoval = nodesWithKnown.withColumn("knownLabels",
      when(rand(123) < dropProbability, typedLit(Seq.empty[String])).otherwise(col("knownLabels"))
    )
    println(s"Nodes after removal (probability of drop = $dropProbability):")
    nodesAfterRemoval.select("_nodeId", "knownLabels").show(10, truncate = false)

    // Create the knownRelationships column
    val EdgesWithKnown = EdgesDF.withColumn("knownRelationships", split(col("relationshipType"), ":"))
    println("Edges with knownRelationships:")
    EdgesWithKnown.select("srcId", "dstId", "knownRelationships").show(10, truncate = false)

    // Remove knownRelationships with a certain probability
    val dropProbabilityEdges = 0.5
    val EdgesAfterRemoval = EdgesWithKnown.withColumn("knownRelationships",
      when(rand(123) < dropProbabilityEdges, typedLit(Seq.empty[String])).otherwise(col("knownRelationships"))
    )
    println(s"Edges after removal (probability of drop = $dropProbabilityEdges):")
    EdgesAfterRemoval.select("srcId", "dstId", "knownRelationships").show(10, truncate = false)

    // Generate binary matrices for LSH
    val binaryMatrixforNodesDF_LSH = Clustering.createBinaryMatrixforNodes(nodesAfterRemoval).cache()
    val binaryMatrixforEdgesDF_LSH = Clustering.createBinaryMatrixforEdges(EdgesAfterRemoval).cache()

    // Return processed DataFrames
    (binaryMatrixforNodesDF_LSH, binaryMatrixforEdgesDF_LSH)
  }
}
