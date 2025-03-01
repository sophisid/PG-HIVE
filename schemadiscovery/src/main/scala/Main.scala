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
      case None => NodePatternRepository.createPattern(label, props, nodeId)
    }
  }

  def processSingleEdge(row: Row): Unit = {
    val edgeId = row.hashCode().toLong
    val label = row.getAs[String]("relationshipType")
    val srcLabel = row.getAs[String]("srcType")
    val dstLabel = row.getAs[String]("dstType")
    val props = row.schema.fields.map(_.name)
      .filterNot(n => n == "srcId" || n == "dstId" || n == "relationshipType" || n == "srcType" || n == "dstType")
      .filter(n => row.getAs[Any](n) != null) // Keep only non-null properties
      .toSet

    EdgePatternRepository.findMatchingPattern(label, srcLabel, dstLabel, props) match {
      case Some(p) => p.assignedEdges += edgeId
      case None => EdgePatternRepository.createPattern(label, srcLabel, dstLabel, props, edgeId)
    }
  }

  def runEvaluationOnCurrentPatterns(patterns: DataFrame): Unit = {
    patterns.show(1000, truncate = false)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("IncrementalPatternMatchingWithBRPLSH").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val nodesDF = DataLoader.loadAllNodes(spark)
    val edgesDF = DataLoader.loadAllRelationships(spark)

    val nodeChunks = chunkDFBySize(nodesDF, 100)
    val edgeChunks = chunkDFBySize(edgesDF, 100)

    nodeChunks.zipWithIndex.foreach { case (chunk, idx) =>
      val previousPatternIds = NodePatternRepository.allPatterns.map(_.patternId).toSet
      chunk.collect().foreach(processSingleNode)
      val newPatterns = NodePatternRepository.allPatterns.filter(p => !previousPatternIds.contains(p.patternId))

      if (newPatterns.nonEmpty) {
        import spark.implicits._
        val newPatternsDF = newPatterns.map(p => (p.patternId, p.label, p.properties.toSeq)).toDF("patternId", "label", "properties")

        val encodedPatterns = PatternPreprocessing.encodePatterns(spark, newPatternsDF)
        val clusteredNodes = LSHClustering.applyLSH(spark, encodedPatterns)
        runEvaluationOnCurrentPatterns(clusteredNodes)
      }
    }

    // TODO edges

    spark.stop()
  }
}
