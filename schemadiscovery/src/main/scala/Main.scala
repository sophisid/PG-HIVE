import org.apache.spark.sql.{DataFrame, SparkSession, Row}
import org.apache.spark.sql.types.{ArrayType, StructType}
import org.apache.spark.sql.functions._
import scala.collection.mutable

object Main { 
  def safeFlattenProps(df: DataFrame): DataFrame = {
    if (df.columns.contains("propsInCluster")) {
      val propsField = df.schema("propsInCluster").dataType
      propsField match {
        case ArrayType(ArrayType(_, _), _) =>
          df.withColumn("propsInCluster", flatten(col("propsInCluster")))
        case _ =>
          df
      }
    } else {
      df
    }
  }

  def safeFlattenPropsPatterns(df: DataFrame): DataFrame = {
    if (df.columns.contains("propertiesInCluster")) {
      val propsField = df.schema("propertiesInCluster").dataType
      propsField match {
        case ArrayType(ArrayType(_, _), _) =>
          df.withColumn("propertiesInCluster", flatten(col("propertiesInCluster")))
        case _ =>
          df
      }
    } else {
      df
    }
  }

  def mergeTwoDFsBySortedLabels(spark: SparkSession, df1: DataFrame, df2: DataFrame): DataFrame = {
    import spark.implicits._

    // Union the two DataFrames
    val unionDF = df1.unionByName(df2)

    // Normalize nested arrays if needed
    val cleanedDF = unionDF
      .withColumn("propertiesInCluster",
        when(size($"propertiesInCluster") > 0, $"propertiesInCluster").otherwise(array(lit("")))
      )
      .withColumn("mandatoryProperties",
        when(size($"mandatoryProperties") > 0, $"mandatoryProperties").otherwise(array(lit("")))
      )
      .withColumn("optionalProperties",
        when(size($"optionalProperties") > 0, $"optionalProperties").otherwise(array(lit("")))
      )

    // Group by sortedLabels and aggregate
    val mergedDF = cleanedDF
      .groupBy($"sortedLabels")
      .agg(
        flatten(collect_set($"nodeIdsInCluster")).as("nodeIdsInCluster"),
        array_distinct(flatten(collect_set($"propertiesInCluster"))).as("propertiesInCluster"),
        array_distinct(flatten(collect_set($"optionalProperties"))).as("optionalProperties"),
        array_distinct(flatten(collect_set($"original_cluster_ids"))).as("original_cluster_ids"),
        array_distinct(aggregate(
          collect_list($"propertiesInCluster"),
          first($"propertiesInCluster", ignoreNulls = true),
          (acc, props) => array_intersect(acc, props)
        )).as("mandatoryProperties")
      )
      .withColumn("merged_cluster_id", concat(lit("merged_by_sorted_labels_"), monotonically_increasing_id()))

    // Ensure schema consistency
    val resultDF = mergedDF.select(
      $"sortedLabels".cast("array<string>"),
      $"nodeIdsInCluster".cast("array<string>"),
      $"propertiesInCluster".cast("array<string>"),
      $"optionalProperties".cast("array<string>"),
      $"original_cluster_ids".cast("array<string>"),
      $"mandatoryProperties".cast("array<string>"),
      $"merged_cluster_id".cast("string")
    )

    println("Schema after merging two DataFrames by sortedLabels:")
    resultDF.printSchema()
    println("Sample after merging:")
    resultDF.show(50)

    resultDF
  }


  def mergeTwoDFsByRelationshipTypes(spark: SparkSession, df1: DataFrame, df2: DataFrame): DataFrame = {
    import spark.implicits._

    // Union the two DataFrames
    val unionDF = df1.union(df2)

    // Clean and prepare the DataFrame
    val cleanedDF = unionDF
      .withColumn("propsInCluster",
        when(size($"propsInCluster") > 0, $"propsInCluster".cast("array<string>"))
          .otherwise(array().cast("array<string>"))
      )
      .withColumn("mandatoryProperties",
        when(size($"propsInCluster") > 0, $"propsInCluster".cast("array<string>"))
          .otherwise(array().cast("array<string>"))
      )

    // Group by relationshipTypes and aggregate
    val mergedDF = cleanedDF
      .groupBy($"relationshipTypes")
      .agg(
        flatten(collect_set($"srcLabels")).as("srcLabels"),
        flatten(collect_set($"dstLabels")).as("dstLabels"),
        array_distinct(flatten(collect_list($"propsInCluster"))).as("propsInCluster"),
        flatten(collect_list($"edgeIdsInCluster")).as("edgeIdsInCluster"),
        array_distinct(aggregate(
          collect_list($"propsInCluster"),
          first($"propsInCluster", ignoreNulls = true),
          (acc, props) => array_intersect(acc, props)
        )).as("mandatoryProperties"),
        array_distinct(flatten(collect_set($"optionalProperties"))).as("optionalProperties"),
        flatten(collect_set($"original_cluster_ids")).as("original_cluster_ids")
      )
      .withColumn("merged_cluster_id", concat(lit("merged_by_reltype_"), monotonically_increasing_id()))

    // Ensure schema consistency
    val resultDF = mergedDF.select(
      $"relationshipTypes".cast("array<string>"),
      $"srcLabels".cast("array<string>"),
      $"dstLabels".cast("array<string>"),
      $"propsInCluster".cast("array<string>"),
      $"edgeIdsInCluster".cast("array<struct<srcId:long,dstId:long>>"),
      $"mandatoryProperties".cast("array<string>"),
      $"optionalProperties".cast("array<string>"),
      $"original_cluster_ids".cast("array<string>"),
      $"merged_cluster_id".cast("string")
    )

    println("Schema after merging two DataFrames by relationshipTypes:")
    resultDF.printSchema()
    println("Sample after merging:")
    resultDF.show(50)

    resultDF
  }
def alignSchemas(df1: DataFrame, df2: DataFrame): (DataFrame, DataFrame) = {
  val df1Cols = df1.columns.toSet
  val df2Cols = df2.columns.toSet

  val allCols = df1Cols union df2Cols

  def addMissingColumns(df: DataFrame, allCols: Set[String]): DataFrame = {
    val currentCols = df.columns.toSet
    val missingCols = allCols.diff(currentCols)
    val dfWithMissingCols = missingCols.foldLeft(df)((acc, colName) => acc.withColumn(colName, lit(null)))
    dfWithMissingCols.select(allCols.toSeq.sorted.map(col): _*)
  }

  val alignedDF1 = addMissingColumns(df1, allCols)
  val alignedDF2 = addMissingColumns(df2, allCols)

  (alignedDF1, alignedDF2)
}


  def main(args: Array[String]): Unit = {
    val clusteringMethod = if (args.length < 1) "LSH" else args(0).toUpperCase()
    //check if clustering incremental
    val incremental = args.length > 1 && args(1).toLowerCase == "incremental"
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
    val startTime = System.currentTimeMillis() 
    spark.sparkContext.setLogLevel("ERROR")
    if(incremental){
      val batchSize = 1000000 // Adjust based on your dataset and memory constraints
      val jaccardThreshold = 0.9
      val computePostProcessing = true
      
      var mergedBatchedPatterns: DataFrame = spark.emptyDataFrame
      var mergedBatchedEdges: DataFrame = spark.emptyDataFrame
      var allNodesAccum: DataFrame = spark.emptyDataFrame
      var allEdgesAccum: DataFrame = spark.emptyDataFrame


      var offset = 0L
      var batchIndex = 0
      var hasMoreData = true

      while(hasMoreData)
      {
        println(s"Loading batch $batchIndex with offset $offset")
        val nodesDF = DataLoader.loadNodesBatch(spark, batchSize, offset)
        val edgesDF = DataLoader.loadRelationshipsBatch(spark, batchSize, offset)
        val (alignedAccum, alignedBatch) = alignSchemas(allNodesAccum, nodesDF)
        allNodesAccum = alignedAccum.unionByName(alignedBatch)
        val (alignedAccumEdges, alignedBatchEdges) = alignSchemas(allEdgesAccum, edgesDF)
        allEdgesAccum = alignedAccumEdges.unionByName(alignedBatchEdges)


        if(nodesDF.isEmpty && edgesDF.isEmpty) {
          hasMoreData = false
        } else {
          val allNodeProperties = nodesDF.columns.filterNot(Seq("_nodeId", "_labels", "originalLabels").contains).toSet    
          val allEdgeProperties = edgesDF.columns.filterNot(Seq("srcId", "dstId", "srcType", "dstType", "relationshipType").contains).toSet

          println(s"Loaded ${nodesDF.count()} nodes and ${edgesDF.count()} edges in batch $batchIndex")
          val binaryNodesDF = PatternPreprocessing.encodePatterns(spark, nodesDF, allNodeProperties)
          val binaryEdgesDF = PatternPreprocessing.encodeEdgePatterns(spark, edgesDF, allEdgeProperties)
          binaryNodesDF.select("_labels").distinct().show(false)
          binaryEdgesDF.select("relationshipTypeArray").distinct().show(false)

          // Cluster nodes LSH 
          val clusteredNodes = LSHClustering.applyLSHNodes(spark, binaryNodesDF)
          val clusteredEdges = LSHClustering.applyLSHEdges(spark, binaryEdgesDF)
          // clusteredNodes.printSchema()
          // clusteredEdges.printSchema()

          val mergedPatterns = LSHClustering.mergePatternsByLabel(spark, clusteredNodes)
          val mergedEdges = LSHClustering.mergeEdgePatternsByLabel(spark, clusteredEdges)



          if(mergedPatterns.isEmpty || batchIndex == 0) {
            mergedBatchedPatterns = mergedPatterns 
          } else {
            // Merge with existing patterns
            // mergedPatterns = mergeByRelationshipTypes(spark, mergedPatterns)
            val cleanedMergedBatchedPatterns = safeFlattenPropsPatterns(mergedBatchedPatterns)
            val cleanedMergedPatterns = safeFlattenPropsPatterns(mergedPatterns)
            mergedBatchedPatterns = mergeTwoDFsBySortedLabels(spark, cleanedMergedBatchedPatterns, cleanedMergedPatterns)
          }
          if(mergedEdges.isEmpty || batchIndex == 0) {
              mergedBatchedEdges = mergedEdges
          } else {
            // Merge with existing edges
             val cleanedMergedBatchedEdges = safeFlattenProps(mergedBatchedEdges)
            val cleanedMergedEdges = safeFlattenProps(mergedEdges)
            mergedBatchedEdges = mergeTwoDFsByRelationshipTypes(spark, cleanedMergedBatchedEdges, cleanedMergedEdges)
            
          }

          // mergedEdges.printSchema()
          // mergedPatterns.printSchema()
          // Incremental processing
          if (computePostProcessing) {
            // Evaluation.computeMetricsForNodes(spark, allNodesAccum, mergedBatchedPatterns)
            // Evaluation.computeMetricsForEdges(spark, allEdgesAccum, mergedBatchedEdges)
            val updatedMergedPatterns = InferSchema.inferPropertyTypesFromMerged(nodesDF, mergedBatchedPatterns, "LSH Merged Nodes", Seq("mandatoryProperties", "optionalProperties"), "_nodeId")
            val updatedMergedEdges = InferSchema.inferPropertyTypesFromMerged(edgesDF, mergedBatchedEdges, "LSH Merged Edges", Seq("mandatoryProperties", "optionalProperties"), "edgeIdsInCluster")
            val updatedMergedEdgesWCardinalities = InferSchema.inferCardinalities(edgesDF, updatedMergedEdges)
            PGSchemaExporterLoose.exportPGSchema(
            updatedMergedPatterns,
            updatedMergedEdgesWCardinalities,
            "pg_schema_output_loose.txt"
            )

            PGSchemaExporterStrict.exportPGSchema(
            updatedMergedPatterns,
            updatedMergedEdgesWCardinalities,
            "pg_schema_output_strict.txt"
            )

            XSDExporter.exportXSD(updatedMergedPatterns, updatedMergedEdgesWCardinalities, "schema_output.xsd")
          }

          offset += batchSize
          batchIndex += 1
        }
      }
      // Final merged patterns and edges
      println("Final Merged Patterns:")
      mergedBatchedPatterns.printSchema()
      mergedBatchedPatterns.show(100)
      println("Final Merged Edges:")
      mergedBatchedEdges.printSchema()
      mergedBatchedEdges.show(100)
      Evaluation.computeMetricsForNodes(spark, allNodesAccum, mergedBatchedPatterns)
      Evaluation.computeMetricsForEdges(spark, allEdgesAccum, mergedBatchedEdges)
    }
    else {
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


        PGSchemaExporterLoose.exportPGSchema(
        updatedMergedPatterns,
        updatedMergedEdgesWCardinalities,
        "pg_schema_output_loose.txt"
        )

        PGSchemaExporterStrict.exportPGSchema(
        updatedMergedPatterns,
        updatedMergedEdgesWCardinalities,
        "pg_schema_output_strict.txt"
        )

        XSDExporter.exportXSD(updatedMergedPatterns, updatedMergedEdgesWCardinalities, "schema_output.xsd")
  
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

    } 
    val endTime = System.currentTimeMillis()
    val elapsedTime = endTime - startTime
    val elapsedTimeInSeconds = elapsedTime / 1000.0
    println(s"Elapsed time for proccesing : $elapsedTimeInSeconds seconds")
    println(s"Elapsed time for proccesing: $elapsedTime milliseconds")
    spark.stop()
  }
}