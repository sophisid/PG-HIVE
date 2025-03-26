import org.apache.spark.sql.{DataFrame, SparkSession, Row}
import org.apache.spark.sql.functions._
import scala.collection.mutable

object Main { 

  def main(args: Array[String]): Unit = {
    val clusteringMethod = if (args.length < 1) "LSH" else args(0).toUpperCase()

    if (!Set("LSH", "KMEANS", "BOTH").contains(clusteringMethod)) {
      println(s"Invalid clustering method: $clusteringMethod. Use LSH, KMEANS, or BOTH.")
      System.exit(1)
    }

    val spark = SparkSession.builder()
      .appName("HybridLSHDemo")
      .master("local[*]")
      .config("spark.executor.memory", "16g")
      .config("spark.driver.memory", "16g")
      .config("spark.executor.cores", "4")
      .config("spark.executor.instances", "10")
      .config("spark.yarn.executor.memoryOverhead", "4g")
      .config("spark.driver.maxResultSize", "4g")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")

    val nodesDF = DataLoader.loadAllNodes(spark)
    val edgesDF = DataLoader.loadAllRelationships(spark)

    println("Nodes Schema:")
    nodesDF.printSchema()
    println("Edges Schema:")
    edgesDF.printSchema()

    // Find distinct properties
    val allNodeProperties = nodesDF.columns.filterNot(Seq("_nodeId", "_labels", "originalLabels").contains).toSet    
    val allEdgeProperties = edgesDF.columns.filterNot(Seq("srcId", "dstId", "srcType", "dstType", "relationshipType").contains).toSet

    // Preprocess nodes & edges 
    val binaryNodesDF = PatternPreprocessing.encodePatterns(spark, nodesDF, allNodeProperties)
    val binaryEdgesDF = PatternPreprocessing.encodeEdgePatterns(spark, edgesDF, allEdgeProperties)    

    if (clusteringMethod == "LSH" || clusteringMethod == "BOTH") {
      // Cluster nodes LSH 
      val clusteredNodes = LSHClustering.applyLSHNodes(spark, binaryNodesDF)
      val clusteredEdges = LSHClustering.applyLSHEdges(spark, binaryEdgesDF)
      clusteredNodes.show()
      clusteredEdges.show()

      val mergedPatterns = LSHClustering.mergePatternsByLabel(spark, clusteredNodes)
      println("Merged Patterns LSH by Label:")
      mergedPatterns.printSchema()
      mergedPatterns.show(5)

      val mergedEdges = LSHClustering.mergeEdgePatternsByLabel(spark, clusteredEdges)
      println("Merged Edges LSH by Label:")
      mergedEdges.printSchema()
      mergedEdges.show(5)

      // Evaluation for LSH
      println("\n=== Evaluation for LSH Nodes ===")
      Evaluation.computeMetricsForNodes(spark, nodesDF, mergedPatterns)
      println("\n=== Evaluation for LSH Edges ===")
      Evaluation.computeMetricsForEdges(spark, edgesDF, mergedEdges)

      val updatedMergedPatterns = InferTypes.inferPropertyTypesFromMerged(nodesDF, mergedPatterns, "LSH Merged Nodes", Seq("mandatoryProperties", "optionalProperties"), "_nodeId")
      val updatedMergedEdges = InferTypes.inferPropertyTypesFromMerged(edgesDF, mergedEdges, "LSH Merged Edges", Seq("mandatoryProperties", "optionalProperties"), "edgeIdsInCluster")

      println("Updated Merged Patterns LSH with Types:")
      updatedMergedPatterns.printSchema()
      updatedMergedPatterns.show(5)

      println("Updated Merged Edges LSH with Types:")
      updatedMergedEdges.printSchema()
      updatedMergedEdges.show(5)
    }

    if (clusteringMethod == "KMEANS" || clusteringMethod == "BOTH") {
      // Cluster nodes KMeans
      val kmeansClusteredNodes = KMeansClustering.applyKMeansNodes(spark, binaryNodesDF, k = 10)
      val mergedKMeansNodes = KMeansClustering.mergePatternsByKMeansLabel(spark, kmeansClusteredNodes)

      val kmeansClusteredEdges = KMeansClustering.applyKMeansEdges(spark, binaryEdgesDF, k = 10)
      val mergedKMeansEdges = KMeansClustering.mergeEdgePatternsByKMeansLabel(spark, kmeansClusteredEdges)
      println("Merged Patterns KMEANS by Label:")
      mergedKMeansNodes.show()
      println("Merged Edges KMEANS by Label:")
      mergedKMeansEdges.show()

      // Evaluation for KMeans
      println("\n=== Evaluation for KMeans Nodes ===")
      Evaluation.computeMetricsForNodes(spark, nodesDF, mergedKMeansNodes)
      println("\n=== Evaluation for KMeans Edges ===")
      Evaluation.computeMetricsForEdges(spark, edgesDF, mergedKMeansEdges)

      val updatedMergedPatterns = InferTypes.inferPropertyTypesFromMerged(nodesDF, mergedKMeansNodes, "LSH Merged Nodes", Seq("mandatoryProperties", "optionalProperties"), "_nodeId")
      val updatedMergedEdges = InferTypes.inferPropertyTypesFromMerged(edgesDF, mergedKMeansEdges, "LSH Merged Edges", Seq("mandatoryProperties", "optionalProperties"), "edgeIdsInCluster")

      println("Updated Merged Patterns LSH with Types:")
      updatedMergedPatterns.printSchema()
      updatedMergedPatterns.show(5)

      println("Updated Merged Edges LSH with Types:")
      updatedMergedEdges.printSchema()
      updatedMergedEdges.show(5)
    }

    spark.stop()
  }
}