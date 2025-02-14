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

    // load data
    val nodesDF = DataLoader.loadAllNodes(spark)
    println("Initial nodes:")
    nodesDF.show(10, truncate = false)

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

    val binaryMatrixDF_LSH = Clustering.createBinaryMatrix(nodesAfterRemoval).cache()

    // LSH Clustering
    val hybridClustersDF = Clustering.computeLSHJaccardPairs(
      binaryMatrixDF_LSH,
      similarityThreshold = 0.8,
      desiredCollisionProbability = 0.9,
      distanceCutoff = 0.2
    )(spark)


    hybridClustersDF.show(1000,truncate = false)

    spark.stop()
  }
}
