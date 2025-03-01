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
    val label = row.getAs[String]("_labels")
    val props = row.schema.fields.map(_.name)
      .filterNot(n => n == "_nodeId" || n == "_labels")
      .filter(n => row.getAs[Any](n) != null)
      .toSet

    NodePatternRepository.findMatchingPattern(label, props) match {
      case Some(p) => p.assignedNodes += nodeId
      case None    => NodePatternRepository.createPattern(label, props, nodeId)
    }
  }

  def processSingleEdge(row: Row): Unit = {
    val edgeId = row.hashCode().toLong
    val relationshipType = row.getAs[String]("relationshipType")
    val srcLabel = row.getAs[String]("srcType")
    val dstLabel = row.getAs[String]("dstType")
    val props = row.schema.fields.map(_.name)
      .filterNot(n => Seq("relationshipType", "srcType", "dstType", "srcId", "dstId").contains(n))
      .filter(n => row.getAs[Any](n) != null)
      .toSet

    EdgePatternRepository.findMatchingPattern(relationshipType, srcLabel, dstLabel, props) match {
      case Some(p) => p.assignedEdges += edgeId
      case None    => EdgePatternRepository.createPattern(relationshipType, srcLabel, dstLabel, props, edgeId)
    }
  }



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
        val clusteredDF = LSHClustering.applyLSHNodes(spark, encodedDF)
        clusteredDF.show(50, truncate = false)
        allPatternsDF.show(50, truncate = false)
      }
    }
    edgeChunks.zipWithIndex.foreach { case (chunk, idx) =>
      val prevIDs = EdgePatternRepository.allPatterns.map(_.patternId).toSet

      // Process each edge row
      chunk.collect().foreach(processSingleEdge)

      val newEdges = EdgePatternRepository.allPatterns.filterNot(e => prevIDs.contains(e.patternId))

      if (newEdges.nonEmpty) {
        import spark.implicits._
        val allEdgesDF = EdgePatternRepository.allPatterns.toSeq.toDF()
        val allProps = EdgePatternRepository.allPatterns.flatMap(_.properties).toSet
        val encodedDF = PatternPreprocessing.encodeEdgePatterns(spark, allEdgesDF, allProps)
        val clusteredEdgesDF = LSHClustering.applyLSHEdges(spark, encodedDF)

        println("\n--- Edges Clustering (Global) ---")
        clusteredEdgesDF.show(50, truncate = false)
        allEdgesDF.show(50, truncate = false)
      }
    }


    spark.stop()
  }
}
