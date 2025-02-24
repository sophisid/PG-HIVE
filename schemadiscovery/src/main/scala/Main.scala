import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("HybridLSHDemo")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
      .config("spark.driver.host", "localhost") // Fix potential hostname issues
      .config("spark.driver.bindAddress", "127.0.0.1") // Prevent binding issues
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    // Load data
    val nodesDF = DataLoader.loadAllNodes(spark)
    val edgesDF = DataLoader.loadAllRelationships(spark)
    val nodesSize = nodesDF.count().toInt
    val edgesSize = edgesDF.count().toInt

    // Preprocess data
    val dropProbability = 0.5
    val (binaryMatrixforNodesDF_LSH, binaryMatrixforEdgesDF_LSH) =
      Preprocessing.preprocessing(spark, nodesDF, edgesDF, dropProbability)

    // LSH Clustering
    val hybridNodes = Clustering.LSHClusteringNodes(
      binaryMatrixforNodesDF_LSH,
      similarityThreshold = 0.8,
      desiredCollisionProbability = 0.9,
      distanceCutoff = 0.2,
      datasetSize = nodesSize
    )(spark)

    // val distinctLabelsPerCluster = Clustering.extractDistinctLabels(hybridNodes)(spark)


    val hybridEdges = Clustering.LSHClusteringEdges(
      binaryMatrixforEdgesDF_LSH,
      similarityThreshold = 0.8,
      desiredCollisionProbability = 0.9,
      distanceCutoff = 0.2,
      datasetSize = edgesSize
    )(spark)

    // hybridNodes.show(1000, truncate = false)
    // hybridEdges.show(1000, truncate = false)
    
    Evaluation.computeMetricsWithoutPairwise(hybridNodes, entityCol = "EntityType")
    Evaluation.computeMetricsWithoutPairwise(hybridEdges, entityCol = "RelationshipType")

    val patternNodesFromJaccard = Clustering.clusterPatternsWithMinHash(hybridNodes, isNode = true)(spark)
    println("\n---- Discovered Patterns from Jaccard ----")
    patternNodesFromJaccard.collect().foreach { row =>
      val types = row.getAs[Seq[Seq[String]]]("ClusteredTypes").flatten.distinct.mkString(", ")
      val patterns = row.getAs[Seq[Seq[String]]]("ClusteredPatterns").flatten.distinct.mkString(", ")

      println(s"$types: $patterns")
    }


    val patternEdgesFromJaccard = Clustering.clusterPatternsWithMinHash(hybridEdges, isNode = false)(spark)
    println("\n---- Discovered Relationships from Jaccard ----")
    patternEdgesFromJaccard.collect().foreach { row =>
      val relationshipTypes = row.getAs[Seq[Seq[String]]]("ClusteredTypes").flatten.distinct.mkString(", ")
      val sourceTypes = row.getAs[Seq[Seq[String]]]("SourceType").flatten.distinct.mkString(", ")
      val destinationTypes = row.getAs[Seq[Seq[String]]]("DestinationType").flatten.distinct.mkString(", ")
      val patterns = row.getAs[Seq[Seq[String]]]("ClusteredPatterns").flatten.distinct.mkString(", ")

      println(s"$relationshipTypes ($sourceTypes → $destinationTypes): $patterns")
    }

    val clusteredNodesForEval = patternNodesFromJaccard
      .withColumn("predictedCluster", sha2(to_json(col("lshHashes")), 256))


    val clusteredEdgesForEval = patternEdgesFromJaccard
      .withColumn("predictedCluster", sha2(to_json(col("lshHashes")), 256))

    
    Evaluation.computeMetricsWithoutPairwiseJaccard(
      clusteredNodesForEval,
      entityCol = "ClusteredTypes",
      predictedCol = "predictedCluster"
    )

    Evaluation.computeMetricsWithoutPairwiseJaccard(
      clusteredEdgesForEval,
      entityCol = "ClusteredTypes",
      predictedCol = "predictedCluster"
    )

    spark.stop()
  }
}
