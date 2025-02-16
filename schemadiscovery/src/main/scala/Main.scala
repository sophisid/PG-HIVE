import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import javax.xml.crypto.Data

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("HybridLSHDemo")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    // load data
    val nodesDF = DataLoader.loadAllNodes(spark)
    val EdgesDF = DataLoader.loadAllRelationships(spark)
    val nodesSize = nodesDF.count().toInt
    val edgesSize = EdgesDF.count().toInt
    println("Initial nodes:")
    nodesDF.show(10, truncate = false)

    println("Initial relationships:")
    EdgesDF.show(10, truncate = false)

    // Create the knownLabels column
    val nodesWithKnown = nodesDF.withColumn("knownLabels", split(col("_labels"), ":"))
    println("Nodes with knownLabels:")
    nodesWithKnown.select("_nodeId", "knownLabels").show(10, truncate = false)

    // Remove knownLabels with a certain probability
    val dropProbability = 0.5
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

    val binaryMatrixforNodesDF_LSH = Clustering.createBinaryMatrixforNodes(nodesAfterRemoval).cache()

    val binaryMatrixforEdgesDF_LSH = Clustering.createBinaryMatrixforEdges(EdgesAfterRemoval).cache()

    // LSH Clustering
    val hybridNodes = Clustering.LSHClusteringNodes(
      binaryMatrixforNodesDF_LSH,
      similarityThreshold = 0.8,
      desiredCollisionProbability = 0.9,
      distanceCutoff = 0.2,
      datasetSize = nodesSize
    )(spark)

    // hybridNodes.show(1000,truncate = false)

    val hybridEdges = Clustering.LSHClusteringEdges(
      binaryMatrixforEdgesDF_LSH,
      similarityThreshold = 0.8,
      desiredCollisionProbability = 0.9,
      distanceCutoff = 0.2,
      datasetSize = edgesSize
    )(spark)

    // hybridEdges.show(1000,truncate = false)

    spark.stop()
  }
}
