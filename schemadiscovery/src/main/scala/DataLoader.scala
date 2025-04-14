import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.neo4j.driver.{AuthTokens, GraphDatabase}
import scala.collection.JavaConverters._

object DataLoader {

  def fileExists(spark: SparkSession, path: String): Boolean = {
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    fs.exists(new Path(path))
  }

  def loadAllNodes(spark: SparkSession): DataFrame = {
    import spark.implicits._
    val uri = "bolt://localhost:7687"
    val user = "neo4j"
    val password = "mypassword"

    val driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password))
    val session = driver.session()

    println("Loading all nodes from Neo4j")

    // The query to return property labels
    val result = session.run("MATCH (n) WITH n, rand() AS random RETURN n, labels(n) AS labels ORDER BY random")
    val nodes = result.list().asScala.map { record =>
      val node = record.get("n").asNode()
      val labels = record.get("labels").asList().asScala.map(_.toString)
      val props = node.asMap().asScala.toMap.map { case (key, value) =>
        val strValue = value match {
          case list: java.util.List[_] =>
            list.asScala.mkString(",")
          case other =>
            other.toString
        }
        key -> strValue
      }


      props + ("_nodeId" -> node.id()) + ("_labels" -> labels.mkString(":")) + ("originalLabels" -> labels)
    }

    session.close()
    driver.close()

    val allKeys = nodes.flatMap(_.keys).toSet
    val fields = allKeys.map {
      case "_nodeId" => StructField("_nodeId", LongType, nullable = false)
      case  "originalLabels" => StructField("originalLabels", ArrayType(StringType), nullable = true)
      case key => StructField(key, StringType, nullable = true)
    }.toArray
    val schema = StructType(fields.toSeq)


    val rows = nodes.map { nodeMap =>
      Row(schema.fields.map(f => Option(nodeMap.getOrElse(f.name, null)).orNull): _*)
    }

    val nodesDF = spark.createDataFrame(spark.sparkContext.parallelize(rows.toSeq), schema)
    println(s"Total nodes loaded: ${nodesDF.count()}")
    nodesDF
  }

  def loadAllRelationships(spark: SparkSession): DataFrame = {
    import spark.implicits._
    val uri = "bolt://localhost:7687"
    val user = "neo4j"
    val password = "mypassword"

    val driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password))
    val session = driver.session()
    println("Loading all relationships from Neo4j")
    val result = session.run(
      """MATCH (n)-[r]->(m)
        |WITH n, r, m, rand() as random
        |RETURN id(n) AS srcId, labels(n) AS srcType,
        |       id(m) AS dstId, labels(m) AS dstType,
        |       type(r) AS relationshipType, properties(r) AS properties
        | ORDER BY random""".stripMargin
    )

    val relationships = result.list().asScala.map { record =>
      val srcId = record.get("srcId").asLong()
      val dstId = record.get("dstId").asLong()
      val srcType = record.get("srcType").asList().asScala.mkString(":")
      val dstType = record.get("dstType").asList().asScala.mkString(":")
      val relationshipType = record.get("relationshipType").asString()
      val properties = record.get("properties").asMap().asScala.toMap.mapValues(_.toString)

      properties + ("srcId" -> srcId, "dstId" -> dstId, "relationshipType" -> relationshipType, "srcType" -> srcType, "dstType" -> dstType)
    }

    session.close()
    driver.close()

    val allKeys = relationships.flatMap(_.keys).toSet
    val fields = allKeys.map {
      case key @ ("srcId" | "dstId") => StructField(key, LongType, nullable = false)
      case key => StructField(key, StringType, nullable = true)
    }.toArray
    val schema = StructType(fields.toSeq)


    val rows = relationships.map { relMap =>
      Row(schema.fields.map(f => Option(relMap.getOrElse(f.name, null)).orNull): _*)
    }

    val relationshipsDF = spark.createDataFrame(spark.sparkContext.parallelize(rows.toSeq), schema)
    println(s"Total relationships loaded: ${relationshipsDF.count()}")
    relationshipsDF
  }
}
