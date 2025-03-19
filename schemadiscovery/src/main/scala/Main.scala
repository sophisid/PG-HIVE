import org.apache.spark.sql.{DataFrame, SparkSession, Row}
import org.apache.spark.sql.functions._
import scala.collection.mutable

object Main {

  def chunkDFBySize(df: DataFrame, chunkSize: Long): Seq[DataFrame] = {
    val totalCount = df.count()
    val withIndex = df.withColumn("__rowId", monotonically_increasing_id())
    val chunks = mutable.ArrayBuffer[DataFrame]()
    var start = 0L
    while (start < totalCount) {
      val end = start + chunkSize
      chunks += withIndex
        .filter(col("__rowId") >= start && col("__rowId") < end)
        .drop("__rowId")
      start = end
    }
    chunks
  }

  def processSingleNode(row: Row): Unit = {
    val nodeId = row.getAs[Long]("_nodeId")
    val label = Set(row.getAs[String]("_labels"))
    val props = row.schema.fields.map(_.name)
      .filterNot(n => n == "_nodeId" || n == "_labels")
      .filter(n => row.getAs[Any](n) != null)
      .toSet

    NodePatternRepository.findMatchingPattern(label, props) match {
      case Some(pattern) => pattern.assignedNodes.put(nodeId, label.head)
      case None          => NodePatternRepository.createPattern(label, props, nodeId, label.head)
    }
  }

  def processSingleEdge(row: Row): Unit = {
    val edgeId = row.hashCode().toLong
    val relationshipType = Set(row.getAs[String]("relationshipType"))
    val srcLabel = Set(row.getAs[String]("srcType"))
    val dstLabel = Set(row.getAs[String]("dstType"))
    val props = row.schema.fields.map(_.name)
      .filterNot(n => Seq("relationshipType","srcType","dstType","srcId","dstId").contains(n))
      .filter(n => row.getAs[Any](n) != null)
      .toSet

    EdgePatternRepository.findMatchingPattern(relationshipType, srcLabel, dstLabel, props) match {
      case Some(pattern) => pattern.assignedEdges.put(edgeId, relationshipType.head)
      case None =>
        EdgePatternRepository.createPattern(relationshipType, srcLabel, dstLabel, props, edgeId, relationshipType.head)
    }
  }

  var finalClusteredNodesDF: DataFrame = _
  var finalClusteredEdgesDF: DataFrame = _

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("IncrementalPatternMatchingWithBRPLSH")
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

    val isIncremental = if (args.nonEmpty) args(0).toBoolean else false
    val (nodeChunks, edgeChunks) =
      if (isIncremental) {
        val chunkSize = if (args.length > 1) args(1).toLong else 10000L
        (chunkDFBySize(nodesDF, chunkSize), chunkDFBySize(edgesDF, chunkSize))
      } else (Seq(nodesDF), Seq(edgesDF))

    var globalProperties = Set[String]()

    nodeChunks.zipWithIndex.foreach { case (chunk, idx) =>
      println(s"\n--- Processing node chunk #$idx ---")

      val prevPatternIds = NodePatternRepository.allPatterns.map(_.patternId).toSet
      chunk.collect().foreach(processSingleNode)

      val newPatterns = NodePatternRepository.allPatterns.filterNot(p => prevPatternIds.contains(p.patternId))
      if (newPatterns.nonEmpty) {
        val allPatternsDF = NodePatternRepository.allPatterns.toSeq.toDF()
        val newGlobalProps = NodePatternRepository.allPatterns.flatMap(_.properties).toSet
        globalProperties = globalProperties union newGlobalProps
        val encodedDF = PatternPreprocessing.encodePatterns(spark, allPatternsDF, globalProperties)
        finalClusteredNodesDF = LSHClustering.applyLSHNodes(spark, encodedDF)
        Evaluation.computeIncrementalMetricsForNodes(finalClusteredNodesDF, "labelsInCluster","hashes","labelsInCluster", idx)
      }
    }

    edgeChunks.zipWithIndex.foreach { case (chunk, idx) =>
      println(s"\n--- Processing edge chunk #$idx ---")

      val prevIDs = EdgePatternRepository.allPatterns.map(_.patternId).toSet
      chunk.collect().foreach(processSingleEdge)

      val newEdges = EdgePatternRepository.allPatterns.filterNot(e => prevIDs.contains(e.patternId))
      if (newEdges.nonEmpty) {
        val allEdgesDF = EdgePatternRepository.allPatterns.toSeq.toDF()
        val allProps = EdgePatternRepository.allPatterns.flatMap(_.properties).toSet
        val encodedDF = PatternPreprocessing.encodeEdgePatterns(spark, allEdgesDF, allProps)
        finalClusteredEdgesDF = LSHClustering.applyLSHEdges(spark, encodedDF)
        Evaluation.computeIncrementalMetricsForEdges(finalClusteredEdgesDF, "hashes","relsInCluster",
          "srcLabelsInCluster","dstLabelsInCluster","propsInCluster", idx)
      }
    }

    println("\n--- Final Clustered Node Patterns ---")
    if (finalClusteredNodesDF != null) finalClusteredNodesDF.show(50, truncate=false)
    println("\n--- Final Schema Node Patterns ---")
    finalClusteredNodesDF.printSchema()

    println("\n--- Final Clustered Edge Patterns ---")
    if (finalClusteredEdgesDF != null) finalClusteredEdgesDF.show(50, truncate=false)
    println("\n--- Final Schema Edge Patterns ---")
    finalClusteredEdgesDF.printSchema()

    println("\n--- Final Evaluation Metrics for Grouped Nodes ---")
    if (finalClusteredNodesDF != null) {
      Evaluation.computeIncrementalMetricsForNodes(finalClusteredNodesDF, "labelsInCluster","hashes","labelsInCluster", -1)
      val groupedNodesDF = PostProcessing.groupPatternsByLabel(finalClusteredNodesDF)
      groupedNodesDF.show(truncate=false)
    }

    println("\n--- Final Evaluation Metrics for Grouped Edges ---")
    if (finalClusteredEdgesDF != null) {
      Evaluation.computeIncrementalMetricsForEdges(finalClusteredEdgesDF,"hashes","relsInCluster",
        "srcLabelsInCluster","dstLabelsInCluster","propsInCluster", -1)
      val groupedEdgesDF = PostProcessing.groupPatternsByEdgeType(finalClusteredEdgesDF)
      groupedEdgesDF.show(truncate=false)
    }

    spark.stop()
  }
}
