import com.fasterxml.jackson.core.{JsonFactory, JsonGenerator}
import java.nio.file.Files
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.neo4j.driver.{AuthTokens, Config, GraphDatabase, Record}
import scala.collection.JavaConverters._

/** Stream a Bolt result to disk, then let Spark scan partitioned files.
  * Neither the Bolt records nor the graph-wide maps/Rows accumulate on the driver.
  * The string conversions and metadata keys match the original DataLoader.
  */
object BoundedGraphLoader {
  val NodesQuery = "MATCH (n) WITH n, rand() AS random RETURN n, labels(n) AS labels ORDER BY random"
  val RelationshipsQuery = """MATCH (n)-[r]->(m)
    |WITH n, r, m, rand() as random
    |RETURN id(n) AS srcId, labels(n) AS srcType,
    |       id(m) AS dstId, labels(m) AS dstType,
    |       type(r) AS relationshipType, properties(r) AS properties
    | ORDER BY random""".stripMargin

  def nodeMap(record: Record): Map[String, Any] = {
    val node = record.get("n").asNode()
    val labels = record.get("labels").asList().asScala.map(_.toString).toVector
    val props = node.asMap().asScala.map { case (key, value) =>
      val text = value match {
        case list: java.util.List[_] => list.asScala.mkString(",")
        case other => other.toString
      }
      key -> text
    }.toMap
    props + ("_nodeId" -> node.id()) + ("_labels" -> labels.mkString(":")) +
      ("originalLabels" -> labels)
  }

  def relationshipMap(record: Record): Map[String, Any] = {
    val props = record.get("properties").asMap().asScala.map {
      case (key, value) => key -> value.toString
    }.toMap
    props + ("srcId" -> record.get("srcId").asLong()) +
      ("dstId" -> record.get("dstId").asLong()) +
      ("relationshipType" -> record.get("relationshipType").asString()) +
      ("srcType" -> record.get("srcType").asList().asScala.mkString(":")) +
      ("dstType" -> record.get("dstType").asList().asScala.mkString(":"))
  }

  def schemaFor(keys: Set[String], nodes: Boolean): StructType = StructType(keys.toSeq.map {
    case "_nodeId" if nodes => StructField("_nodeId", LongType, nullable = false)
    case "originalLabels" if nodes => StructField("originalLabels", ArrayType(StringType), nullable = true)
    case key @ ("srcId" | "dstId") if !nodes => StructField(key, LongType, nullable = false)
    case key => StructField(key, StringType, nullable = true)
  })

  private def writeValue(out: JsonGenerator, value: Any): Unit = value match {
    case null => out.writeNull()
    case text: String => out.writeString(text)
    case number: Long => out.writeNumber(number)
    case values: Seq[_] =>
      out.writeStartArray()
      values.foreach {
        case text: String => out.writeString(text)
        case other => throw new IllegalArgumentException("Unexpected label value: " + other)
      }
      out.writeEndArray()
    case other => throw new IllegalArgumentException("Unexpected loader value: " + other.getClass)
  }

  def stage(spark: SparkSession, records: Iterator[Map[String, Any]], nodes: Boolean): DataFrame = {
    val kind = if (nodes) "nodes" else "relationships"
    val directory = MemoryStages.newDirectory(kind)
    val raw = directory.resolve("records.jsonl")
    var keys = Set.empty[String]
    var count = 0L
    var success = false
    try {
      val out = new JsonFactory().createGenerator(Files.newOutputStream(raw))
      try {
        while (records.hasNext) {
          val record = records.next()
          keys ++= record.keySet
          out.writeStartObject()
          record.foreach { case (key, value) => out.writeFieldName(key); writeValue(out, value) }
          out.writeEndObject()
          out.writeRaw('\n')
          count += 1
          if (count % 1000000 == 0) println(s"[bounded-loader] Streamed $count $kind")
        }
      } finally out.close()
      val schema = schemaFor(keys, nodes)
      if (count == 0) {
        MemoryStages.remove(directory)
        success = true
        spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
      } else {
        val loaded = spark.read.schema(schema).option("mode", "FAILFAST").json(raw.toString)
        val parquet = directory.resolve("data.parquet").toString
        loaded.write.mode("errorifexists").parquet(parquet)
        Files.delete(raw)
        // File readers relax nullability. Restore the original metadata schema
        // using a distributed Row RDD, never a driver-side collection.
        val result = spark.createDataFrame(spark.read.schema(schema).parquet(parquet).rdd, schema)
        val actual = result.count()
        require(actual == count, s"Staging lost $kind: wrote $count, read $actual")
        println(s"Total $kind loaded: $actual (disk-backed; no graph-wide driver collection)")
        success = true
        result
      }
    } finally if (!success) MemoryStages.remove(directory)
  }

  def load(spark: SparkSession, nodes: Boolean): DataFrame = {
    val driver = GraphDatabase.driver(
      sys.env.getOrElse("NEO4J_URI", "bolt://localhost:7687"),
      AuthTokens.basic(sys.env.getOrElse("NEO4J_USER", "neo4j"),
        sys.env.getOrElse("NEO4J_PASSWORD", "password")),
      Config.builder().withFetchSize(1000).build())
    try {
      val session = driver.session()
      try {
        println("Streaming all " + (if (nodes) "nodes" else "relationships") + " from Neo4j")
        val result = session.run(if (nodes) NodesQuery else RelationshipsQuery)
        val records = new Iterator[Map[String, Any]] {
          def hasNext: Boolean = result.hasNext
          def next(): Map[String, Any] = if (nodes) nodeMap(result.next()) else relationshipMap(result.next())
        }
        stage(spark, records, nodes)
      } finally session.close()
    } finally driver.close()
  }
}
