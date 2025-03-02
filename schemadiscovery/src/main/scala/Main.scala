import org.apache.spark.sql.{DataFrame, SparkSession, Row}
import org.apache.spark.sql.functions._
import scala.collection.mutable

object Main {
  def chunkDFBySize(df: DataFrame, chunkSize: Long): Seq[DataFrame] = {
    val totalCount = df.count()
    val dfWithIndex = df.withColumn("__rowId", monotonically_increasing_id())
    val chunks = mutable.ArrayBuffer[DataFrame]()
    var start = 0L
    while (start < totalCount) {
      val end = start + chunkSize
      val chunk = dfWithIndex.filter(col("__rowId") >= start && col("__rowId") < end).drop("__rowId")
      chunks += chunk
      start = end
    }
    chunks
  }

  def processSingleNode(row: Row): Unit = {
    val nodeId = row.getAs[Long]("_nodeId")
    val label = Set(row.getAs[String]("_labels")) // Convert to Set[String]
    val props = row.schema.fields.map(_.name)
      .filterNot(n => n == "_nodeId" || n == "_labels")
      .filter(n => row.getAs[Any](n) != null)
      .toSet

    NodePatternRepository.findMatchingPattern(label, props) match {
      case Some(p) => p.assignedNodes.put(nodeId, label.head) // Use .head to get the first element of the Set
      case None    => NodePatternRepository.createPattern(label, props, nodeId, label.head)
    }
  }

  def processSingleEdge(row: Row): Unit = {
    val edgeId = row.hashCode().toLong
    val relationshipType = Set(row.getAs[String]("relationshipType")) // Convert to Set[String]
    val srcLabel = Set(row.getAs[String]("srcType")) // Convert to Set[String]
    val dstLabel = Set(row.getAs[String]("dstType")) // Convert to Set[String]
    val props = row.schema.fields.map(_.name)
      .filterNot(n => Seq("relationshipType", "srcType", "dstType", "srcId", "dstId").contains(n))
      .filter(n => row.getAs[Any](n) != null)
      .toSet

    EdgePatternRepository.findMatchingPattern(relationshipType, srcLabel, dstLabel, props) match {
      case Some(p) => p.assignedEdges.put(edgeId, relationshipType.head) // Use .head to get the first element of the Set
      case None    => EdgePatternRepository.createPattern(relationshipType, srcLabel, dstLabel, props, edgeId, relationshipType.head)
    }
  }



  var finalClusteredNodesDF: DataFrame = _
  var finalClusteredEdgesDF: DataFrame = _

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("IncrementalPatternMatchingWithBRPLSH")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val nodesDF = DataLoader.loadAllNodes(spark)
    val edgesDF = DataLoader.loadAllRelationships(spark)

    val nodeChunks = chunkDFBySize(nodesDF, 50)
    val edgeChunks = chunkDFBySize(edgesDF, 50)

    var globalProperties = Set[String]()

    nodeChunks.zipWithIndex.foreach { case (chunk, idx) =>
      println(s"\n--- Processing chunk #$idx ---")
      val previousPatternIds = NodePatternRepository.allPatterns.map(_.patternId).toSet
      chunk.collect().foreach(processSingleNode)
      val newPatterns = NodePatternRepository.allPatterns.filterNot(p => previousPatternIds.contains(p.patternId))

      if (newPatterns.isEmpty) {
        println("No new patterns in this chunk. Skipping clustering.")
      } else {
        import spark.implicits._
        val allPatternsDF = NodePatternRepository.allPatterns.toSeq.toDF()
        val newGlobalProps = NodePatternRepository.allPatterns.flatMap(_.properties).toSet
        globalProperties = globalProperties union newGlobalProps
        val encodedDF = PatternPreprocessing.encodePatterns(spark, allPatternsDF, globalProperties)
        finalClusteredNodesDF = LSHClustering.applyLSHNodes(spark, encodedDF) // Αποθήκευση τελικών node patterns
        Evaluation.computeIncrementalMetricsForNodes(finalClusteredNodesDF, "labelsInCluster", "hashes", "labelsInCluster", idx)
      }
    }

    edgeChunks.zipWithIndex.foreach { case (chunk, idx) =>
      val prevIDs = EdgePatternRepository.allPatterns.map(_.patternId).toSet
      chunk.collect().foreach(processSingleEdge)

      val newEdges = EdgePatternRepository.allPatterns.filterNot(e => prevIDs.contains(e.patternId))

      if (newEdges.nonEmpty) {
        import spark.implicits._
        val allEdgesDF = EdgePatternRepository.allPatterns.toSeq.toDF()
        val allProps = EdgePatternRepository.allPatterns.flatMap(_.properties).toSet
        val encodedDF = PatternPreprocessing.encodeEdgePatterns(spark, allEdgesDF, allProps)
        finalClusteredEdgesDF = LSHClustering.applyLSHEdges(spark, encodedDF) // Αποθήκευση τελικών edge patterns
        Evaluation.computeIncrementalMetricsForEdges(finalClusteredEdgesDF, "hashes", "relsInCluster", "srcLabelsInCluster", "dstLabelsInCluster", "propsInCluster", idx)
      }
    }

    // Print final clustered node patterns
    println("\n--- Final Clustered Node Patterns ---")
    finalClusteredNodesDF.show(50, truncate = false)

    // Print final clustered edge patterns
    println("\n--- Final Clustered Edge Patterns ---")
    finalClusteredEdgesDF.show(50, truncate = false)

    // Compute final evaluation metrics for nodes
    println("\n--- Final Evaluation Metrics for Nodes ---")
    Evaluation.computeIncrementalMetricsForNodes(finalClusteredNodesDF, "labelsInCluster", "hashes", "labelsInCluster", -1)

    // Compute final evaluation metrics for edges
    println("\n--- Final Evaluation Metrics for Edges ---")
    Evaluation.computeIncrementalMetricsForEdges(finalClusteredEdgesDF, "hashes", "relsInCluster", "srcLabelsInCluster", "dstLabelsInCluster", "propsInCluster", -1)

    spark.stop()
  }
}

