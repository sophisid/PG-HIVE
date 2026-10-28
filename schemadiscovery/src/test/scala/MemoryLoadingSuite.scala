import java.nio.charset.StandardCharsets
import java.nio.file.Files
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import scala.collection.JavaConverters._

class MemoryLoadingSuite extends munit.FunSuite {
  var spark: SparkSession = _
  override def beforeAll(): Unit = {
    spark = SparkSession.builder().appName("PG-HIVE bounded-loader equivalence")
      .master("local[2]").config("spark.ui.enabled", "false")
      .config("spark.sql.shuffle.partitions", "2")
      .config("spark.driver.host", "127.0.0.1").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }
  override def afterEach(context: AfterEach): Unit = {
    spark.catalog.clearCache()
    MemoryStages.cleanup()
  }
  override def afterAll(): Unit = spark.stop()

  def legacy(records: Seq[Map[String, Any]], nodes: Boolean): DataFrame = {
    val schema = BoundedGraphLoader.schemaFor(records.flatMap(_.keys).toSet, nodes)
    val rows = records.map(record => Row.fromSeq(schema.fields.map(f => record.getOrElse(f.name, null)).toSeq))
    spark.createDataFrame(spark.sparkContext.parallelize(rows, 1), schema)
  }

  def sameRows(a: DataFrame, b: DataFrame): Unit = {
    assertEquals(a.schema, b.schema)
    assertEquals(a.exceptAll(b).count(), 0L)
    assertEquals(b.exceptAll(a).count(), 0L)
  }

  test("disk-backed nodes preserve IDs, missing values, labels and escaped strings") {
    val records = Seq(
      Map[String, Any]("_nodeId" -> 1L, "_labels" -> "Actor:Person", "originalLabels" -> Seq("Actor", "Person"),
        "name" -> "\"Άλφα\"\nsecond line", "array_text" -> "a,b", "a.b" -> "dotted"),
      Map[String, Any]("_nodeId" -> 2L, "_labels" -> "", "originalLabels" -> Seq.empty[String],
        "late_key" -> "", "array_text" -> ""))
    sameRows(BoundedGraphLoader.stage(spark, records.iterator, nodes = true), legacy(records, nodes = true))
  }

  test("disk-backed edges preserve endpoints, string properties and relationship labels") {
    val records = Seq(
      Map[String, Any]("srcId" -> 1L, "dstId" -> 2L, "srcType" -> "Person", "dstType" -> "",
        "relationshipType" -> "KNOWS", "items" -> "[a, b]", "weight" -> "1.25"),
      Map[String, Any]("srcId" -> 2L, "dstId" -> 1L, "srcType" -> "", "dstType" -> "Person",
        "relationshipType" -> "KNOWS", "when" -> "2024-01-01"))
    sameRows(BoundedGraphLoader.stage(spark, records.iterator, nodes = false), legacy(records, nodes = false))
  }

  test("one-pass stream retains a property first encountered in its final record") {
    val count = 20000
    val stream = Iterator.range(0, count).map { index =>
      val base = Map[String, Any]("_nodeId" -> index.toLong, "_labels" -> "N", "originalLabels" -> Seq("N"))
      if (index == count - 1) base + ("last_only" -> "retained") else base
    }
    val df = BoundedGraphLoader.stage(spark, stream, nodes = true)
    assertEquals(df.count(), count.toLong)
    assertEquals(df.filter(col("last_only").isNotNull).select("_nodeId").first().getLong(0), (count - 1).toLong)
  }

  test("empty streams retain the original empty DataFrame schema") {
    for (nodes <- Seq(true, false)) {
      val df = BoundedGraphLoader.stage(spark, Iterator.empty, nodes)
      assertEquals(df.schema, StructType(Seq.empty[StructField]))
      assertEquals(df.count(), 0L)
    }
  }

  test("partial input failure is propagated instead of producing a truncated graph") {
    val stream = new Iterator[Map[String, Any]] {
      var read = false
      def hasNext: Boolean = true
      def next(): Map[String, Any] = {
        if (read) throw new IllegalStateException("input failure")
        read = true
        Map("_nodeId" -> 1L, "_labels" -> "N", "originalLabels" -> Seq("N"))
      }
    }
    interceptMessage[IllegalStateException]("input failure") {
      BoundedGraphLoader.stage(spark, stream, nodes = true)
    }
  }

  test("materialized checkpoint survives parent cache release and source deletion") {
    val file = Files.createTempFile("checkpoint-input-", ".jsonl")
    try {
      Files.write(file, "{\"value\":1}\n{\"value\":2}\n".getBytes(StandardCharsets.UTF_8))
      val parent = spark.read.json(file.toString).cache()
      assertEquals(parent.count(), 2L)
      val saved = MemoryStages.snapshot(parent)
      parent.unpersist(blocking = true)
      Files.delete(file)
      assertEquals(saved.select(sum("value")).first().getLong(0), 3L)
    } finally Files.deleteIfExists(file)
  }

  test("cleanup refuses to remove a directory belonging to another caller") {
    val unrelated = Files.createTempDirectory("unrelated-input-")
    try {
      intercept[IllegalArgumentException] { MemoryStages.remove(unrelated) }
      assert(Files.isDirectory(unrelated))
    } finally Files.delete(unrelated)
  }

  def canonical(value: Any): String = value match {
    case null => "null"
    case row: Row => row.toSeq.map(canonical).mkString("(", ",", ")")
    case values: Seq[_] => values.map(canonical).sorted.mkString("[", ",", "]")
    case other => other.toString
  }

  def normalized(df: DataFrame): Set[Map[String, String]] = df.collect().map { row =>
    df.schema.fields.zipWithIndex.filterNot { case (field, _) =>
      field.name.toLowerCase.contains("cluster_id")
    }.map { case (field, index) => field.name -> canonical(row.get(index)) }.toMap
  }.toSet

  def infer(nodes: DataFrame, edges: DataFrame, minhash: Boolean, checkpointed: Boolean): Seq[Set[Map[String, String]]] = {
    val nodeKeys = nodes.columns.filterNot(Set("_nodeId", "_labels", "originalLabels")).toSet
    val edgeKeys = edges.columns.filterNot(Set("srcId", "dstId", "srcType", "dstType", "relationshipType")).toSet
    val encodedNodes = PatternPreprocessing.encodePatterns(spark, nodes, nodeKeys)
    val encodedEdges = PatternPreprocessing.encodeEdgePatterns(spark, edges, edgeKeys)
    val clusteredNodes = if (minhash) LSHMinHashClustering.applyLSHNodes(spark, encodedNodes)
                         else LSHClustering.applyLSHNodes(spark, encodedNodes)
    val clusteredEdges = if (minhash) LSHMinHashClustering.applyLSHEdges(spark, encodedEdges)
                         else LSHClustering.applyLSHEdges(spark, encodedEdges)
    def save(df: DataFrame): DataFrame = if (checkpointed) MemoryStages.snapshot(df) else df
    val mergedNodes = save(LSHClustering.mergePatternsByLabel(spark, clusteredNodes))
    val mergedLabels = save(LSHClustering.mergeEdgePatternsByEdgeLabel(spark, clusteredEdges))
    val mergedEdges = save(LSHClustering.mergeEdgePatternsByLabel(spark, clusteredEdges))
    if (checkpointed) clusteredEdges.unpersist(blocking = true)
    val outputs = Seq(mergedNodes, mergedLabels, mergedEdges).map(normalized)
    clusteredEdges.unpersist(blocking = true)
    outputs
  }

  for (minhash <- Seq(false, true)) {
    test(s"${if (minhash) "MinHash" else "LSH"} partition and property equivalence after disk staging/checkpointing") {
      val nodes = (0 until 30).map { i =>
        val label = if (i % 2 == 0) "Actor" else "Movie"
        Map[String, Any]("_nodeId" -> i.toLong, "_labels" -> label, "originalLabels" -> Seq(label),
                         "name" -> s"value-$i", "year" -> "2020")
      }
      val edges = (0 until 30).map { i =>
        Map[String, Any]("srcId" -> i.toLong, "dstId" -> ((i + 1) % 30).toLong,
          "srcType" -> (if (i % 2 == 0) "Actor" else "Movie"),
          "dstType" -> (if (i % 2 == 0) "Movie" else "Actor"),
          "relationshipType" -> "LINK", "weight" -> "1")
      }
      val expected = infer(legacy(nodes, true), legacy(edges, false), minhash, checkpointed = false)
      val actual = infer(BoundedGraphLoader.stage(spark, nodes.iterator, true),
                         BoundedGraphLoader.stage(spark, edges.iterator, false), minhash, checkpointed = true)
      assertEquals(actual, expected)
    }
  }
}
