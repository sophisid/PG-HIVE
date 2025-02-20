import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("HybridLSHDemo")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    // Load data
    val nodesDF = DataLoader.loadAllNodes(spark)
    val EdgesDF = DataLoader.loadAllRelationships(spark)
    val nodesSize = nodesDF.count().toInt
    val edgesSize = EdgesDF.count().toInt

    // Preprocess data
    val dropProbability = 0.5
    val (binaryMatrixforNodesDF_LSH, binaryMatrixforEdgesDF_LSH, dropProbability) =
      Preprocessing.preprocessing(spark, nodesDF, EdgesDF)

    // LSH Clustering
    val hybridNodes = Clustering.LSHClusteringNodes(
      binaryMatrixforNodesDF_LSH,
      similarityThreshold = 0.8,
      desiredCollisionProbability = 0.9,
      distanceCutoff = 0.2,
      datasetSize = nodesSize
    )(spark)

    val hybridEdges = Clustering.LSHClusteringEdges(
      binaryMatrixforEdgesDF_LSH,
      similarityThreshold = 0.8,
      desiredCollisionProbability = 0.9,
      distanceCutoff = 0.2,
      datasetSize = edgesSize
    )(spark)

    // hybridNodes.show(1000, truncate = false)
    // hybridEdges.show(1000, truncate = false)

    spark.stop()
  }
}
