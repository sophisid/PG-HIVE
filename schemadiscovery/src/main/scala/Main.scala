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

  def runEvaluationOnCurrentPatterns(df: DataFrame): Unit = {
    println("\n--- Current Clustering ---")
    df.show(50, truncate = false)
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("IncrementalPatternMatchingWithBRPLSH")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val nodesDF = DataLoader.loadAllNodes(spark)

    val nodeChunks = chunkDFBySize(nodesDF, 2)

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
        val clusteredDF = LSHClustering.applyLSH(spark, encodedDF)
        runEvaluationOnCurrentPatterns(clusteredDF)
      }
    }

    spark.stop()
  }
}
