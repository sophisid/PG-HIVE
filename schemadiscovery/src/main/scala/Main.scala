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
    //calculate time for clustering
    val startTime = System.currentTimeMillis()  

    if (clusteringMethod == "LSH" || clusteringMethod == "BOTH") {
      // Cluster nodes LSH 
      val clusteredNodes = LSHClustering.applyLSHNodes(spark, binaryNodesDF)
      val clusteredEdges = LSHClustering.applyLSHEdges(spark, binaryEdgesDF)
      clusteredNodes.select("labelsInCluster", "propertiesInCluster").show(500, truncate = false)
      clusteredEdges.show()
      //calculate time for clustering
      val startClusteringTime = System.currentTimeMillis()
      val mergedPatterns = LSHClustering.mergePatternsByLabel(spark, clusteredNodes)
      val endClusteringTime = System.currentTimeMillis()
      val elapsedClusteringTime = endClusteringTime - startClusteringTime
      val elapsedClusteringTimeInSeconds = elapsedClusteringTime / 1000.0
      println(s"Elapsed time for clustering : $elapsedClusteringTimeInSeconds seconds")
      println(s"Elapsed time for clustering: $elapsedClusteringTime milliseconds")
      println("Merged Patterns LSH by Label:")
      mergedPatterns.printSchema()
      mergedPatterns.select("sortedLabels", "propertiesInCluster","optionalProperties","mandatoryProperties" ,"original_cluster_ids").show(500)

      val mergedEdgesLabelOnly = LSHClustering.mergeEdgePatternsByEdgeLabel(spark, clusteredEdges)
      println("Merged Edges LSH by Label Only:")
      mergedEdgesLabelOnly.printSchema()
      mergedEdgesLabelOnly.select("relationshipTypes","srcLabels","dstLabels","propsInCluster","merged_cluster_id").show(truncate = false , numRows = 500)

      val mergedEdges = LSHClustering.mergeEdgePatternsByLabel(spark, clusteredEdges)
      println("Merged Edges LSH by Label & SRC/DST:")
      mergedEdges.printSchema()
      mergedEdges.select("relationshipTypes","srcLabels","dstLabels","propsInCluster","merged_cluster_id").show(truncate = false , numRows = 500)

      // Evaluation for LSH
      println("\n=== Evaluation for LSH Nodes ===")
      Evaluation.computeMetricsForNodes(spark, nodesDF, mergedPatterns)
      println("\n=== Evaluation for LSH Edges (LABEL MERGED)===")
      Evaluation.computeMetricsForEdges(spark, edgesDF, mergedEdgesLabelOnly)

      println("\n=== Evaluation for LSH Edges (FULLY MERGED)===")
      Evaluation.computeMetricsForEdges(spark, edgesDF, mergedEdges)

      val updatedMergedPatterns = InferSchema.inferPropertyTypesFromMerged(nodesDF, mergedPatterns, "LSH Merged Nodes", Seq("mandatoryProperties", "optionalProperties"), "_nodeId")
      val updatedMergedEdges = InferSchema.inferPropertyTypesFromMerged(edgesDF, mergedEdgesLabelOnly, "LSH Merged Edges", Seq("mandatoryProperties", "optionalProperties"), "edgeIdsInCluster")

      println("Updated Merged Patterns LSH with Types:")
      // updatedMergedPatterns.printSchema()
      updatedMergedPatterns.show(100)

      println("Updated Merged Edges LSH with Types:")
      // updatedMergedEdges.printSchema()
      updatedMergedEdges.show(100)

      val updatedMergedEdgesWCardinalities = InferSchema.inferCardinalities(edgesDF, updatedMergedEdges)
      println("Updated Merged Edges LSH with Types and Cardinalities:")
      // updatedMergedEdgesWCardinalities.printSchema()
      updatedMergedEdgesWCardinalities.show(5)


      PGSchemaExporter.exportPGSchema(
      updatedMergedPatterns,
      updatedMergedEdgesWCardinalities,
      "pg_schema_output_loose.txt"
      )

      PGSchemaExporterStrict.exportPGSchema(
      updatedMergedPatterns,
      updatedMergedEdgesWCardinalities,
      "pg_schema_output_strict.txt"
      )
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

      val updatedMergedPatterns = InferSchema.inferPropertyTypesFromMerged(nodesDF, mergedKMeansNodes, "LSH Merged Nodes", Seq("mandatoryProperties", "optionalProperties"), "_nodeId")
      val updatedMergedEdges = InferSchema.inferPropertyTypesFromMerged(edgesDF, mergedKMeansEdges, "LSH Merged Edges", Seq("mandatoryProperties", "optionalProperties"), "edgeIdsInCluster")

      println("Updated Merged Patterns LSH with Types:")
      updatedMergedPatterns.printSchema()
      updatedMergedPatterns.show(5)

      println("Updated Merged Edges LSH with Types:")
      updatedMergedEdges.printSchema()
      updatedMergedEdges.show(5)
      val updatedMergedEdgesWCardinalities = InferSchema.inferCardinalities(edgesDF, updatedMergedEdges)
      println("Updated Merged Edges KMeans with Types and Cardinalities:")
      updatedMergedEdgesWCardinalities.printSchema()
      updatedMergedEdgesWCardinalities.show(5)
    }
    val endTime = System.currentTimeMillis()
    val elapsedTime = endTime - startTime
    val elapsedTimeInSeconds = elapsedTime / 1000.0
    println(s"Elapsed time for proccesing : $elapsedTimeInSeconds seconds")
    println(s"Elapsed time for proccesing: $elapsedTime milliseconds")
    spark.stop()
  }
}