import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.neo4j.driver.{AuthTokens, GraphDatabase}
import scala.collection.JavaConverters._
import scala.util.Random


object DataLoader {
  def addNoise(properties: Map[String, Any], noisePercentage: Double): Map[String, Any] = {
  properties.map { case (key, value) =>
    if (Random.nextDouble() < noisePercentage) (key, null) else (key, value)
  }
}
  // Function to load all nodes without labels
  def loadAllNodes(spark: SparkSession,noisePercentage: Double): DataFrame = {
    import spark.implicits._

    val uri = "bolt://localhost:7687"
    val user = "neo4j"
    val password = "mypassword"

    val driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password))
    val session = driver.session()

    println("Loading all nodes from Neo4j")

    // The query to return property labels
    val result = session.run("MATCH (n) WITH n, rand() AS random RETURN n, labels(n) AS labels ORDER BY random ")
    val nodes = result.list().asScala.map { record =>
      val node = record.get("n").asNode()
      val labels = record.get("labels").asList().asScala.map(_.toString)
      val props = node.asMap().asScala.toMap
      // Include node ID and labels
      // Store _nodeId as Long
      val noisyProps = addNoise(props, noisePercentage / 100) // Apply noise
      noisyProps + ("_nodeId" -> node.id()) + ("_labels" -> labels.mkString(":"))
    }

    session.close()
    driver.close()

    // Collect all unique keys to define the schema
    val allKeys = nodes.flatMap(_.keys).toSet

    // Define the schema based on the keys
    val fields = allKeys.map { key =>
      if (key == "_nodeId") {
        StructField(key, LongType, nullable = false)
      } else {
        StructField(key, StringType, nullable = true)
      }
    }.toArray
    val schema = StructType(fields)

    // Convert list of Maps to DataFrame
    val rows = nodes.map { nodeMap =>
      val values = schema.fields.map { field =>
        Option(nodeMap.getOrElse(field.name, null)).map { value =>
          if (field.name == "_nodeId") {
            value.asInstanceOf[Long]
          } else {
            value.toString
          }
        }.orNull
      }
      Row(values: _*)
    }

    val nodesDF = spark.createDataFrame(spark.sparkContext.parallelize(rows.toSeq), schema)
    println(s"Total nodes loaded: ${nodesDF.count()}")
    println("Schema of nodesDF:")
    nodesDF.printSchema()
    nodesDF
  }

  def loadAllRelationships(spark: SparkSession,noisePercentage: Double): DataFrame = {
      import spark.implicits._

      val uri = "bolt://localhost:7687"
      val user = "neo4j"
      val password = "mypassword"

      val driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password))
      val session = driver.session()

      val result = session.run(
        """MATCH (n)-[r]->(m)
          |WITH n, r, m, rand() AS random
          |RETURN id(n) AS srcId, labels(n) AS srcType, 
          |       id(m) AS dstId, labels(m) AS dstType, 
          |       type(r) AS relationshipType, properties(r) AS properties
          |ORDER BY random """.stripMargin
      )

      val relationships = result.list().asScala.map { record =>
        val srcId = record.get("srcId").asLong()
        val dstId = record.get("dstId").asLong()
        val srcType = record.get("srcType").asList().asScala.mkString(":")
        val dstType = record.get("dstType").asList().asScala.mkString(":")  
        val relationshipType = record.get("relationshipType").asString()
        val properties = record.get("properties").asMap().asScala.toMap.mapValues(_.toString)

        val noisyProperties = addNoise(properties, noisePercentage / 100) // Apply noise   
        noisyProperties + ("srcId" -> srcId, "dstId" -> dstId, "relationshipType" -> relationshipType, "srcType" -> srcType, "dstType" -> dstType)
      }

      session.close()
      driver.close()

      val allKeys = relationships.flatMap(_.keys).toSet

      val fields = allKeys.map { key =>
        if (key == "srcId" || key == "dstId") StructField(key, LongType, nullable = false)
        else StructField(key, StringType, nullable = true)
      }.toArray
      val schema = StructType(fields)

      val rows = relationships.map { relMap =>
        val values = schema.fields.map { field =>
          Option(relMap.getOrElse(field.name, null)).orNull
        }
        Row(values: _*)
      }

      val relationshipsDF = spark.createDataFrame(spark.sparkContext.parallelize(rows.toSeq), schema)

      println(s"Total relationships loaded: ${relationshipsDF.count()}")
      relationshipsDF.printSchema()
      relationshipsDF
  }
}

