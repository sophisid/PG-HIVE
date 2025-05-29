import org.apache.spark.sql.{DataFrame, SparkSession, Row}
import org.apache.spark.sql.functions._
import scala.xml._

object XSDToXMLExporter {

  case class ComplexTypeDef(name: String, kind: String, fields: Seq[(String, String)])

  def parseXSD(path: String): Seq[ComplexTypeDef] = {
    val xsd = XML.loadFile(path)
    (xsd \\ "complexType").map { ct =>
      val name = (ct \ "@name").text
      val isNode = (ct \\ "appinfo").text.contains("node")
      val isEdge = (ct \\ "appinfo").text.contains("edge")
      val fields = (ct \\ "sequence" \\ "element").map { el =>
        val fname = (el \\ "@name").text
        val ftype = (el \\ "@type").text
        (fname, ftype)
      }
      ComplexTypeDef(name, if (isNode) "node" else if (isEdge) "edge" else "unknown", fields)
    }
  }

  def exportToXMLFromDataframes(
    spark: SparkSession,
    xsdPath: String,
    outputPath: String,
    mergedPatterns: DataFrame,
    mergedEdges: DataFrame,
    allNodesDF: DataFrame,
    allEdgesDF: DataFrame
  ): Unit = {

    val complexTypes = parseXSD(xsdPath)

    val nodeIdToProps: Map[Long, Map[String, String]] = allNodesDF.collect().map { row =>
      val id = row.getAs[Long]("_nodeId")
      val props = row.schema.fields
        .filterNot(f => Set("_nodeId", "_labels", "originalLabels").contains(f.name))
        .map(f => f.name -> Option(row.getAs[Any](f.name)).map(_.toString).getOrElse(""))
        .toMap
      id -> props
    }.toMap

    val edgeIdToSrcDst: Map[(Long, Long), Map[String, String]] = allEdgesDF.collect().map { row =>
      val src = row.getAs[Long]("srcId")
      val dst = row.getAs[Long]("dstId")
      val srcType = row.getAs[String]("srcType")
      val dstType = row.getAs[String]("dstType")
      val props = row.schema.fields
        .filterNot(f => Set("srcId", "dstId", "srcType", "dstType", "relationshipType").contains(f.name))
        .map(f => f.name -> Option(row.getAs[Any](f.name)).map(_.toString).getOrElse(""))
        .toMap ++ Map(
          "srcId" -> src.toString,
          "dstId" -> dst.toString,
          "srcType" -> srcType,
          "dstType" -> dstType
        )
      (src, dst) -> props
    }.toMap

    val groupedNodeElems = complexTypes.filter(_.kind == "node").map { nodeType =>
      val matchingDF = mergedPatterns.filter(array_contains(col("sortedLabels"), nodeType.name))
      val nodes = matchingDF.collect().flatMap { row =>
        val nodeIds = row.getAs[Seq[Any]]("nodeIdsInCluster")
        val propsWithTypes = row.getAs[Seq[String]]("mandatoryProperties_with_types") ++ row.getAs[Seq[String]]("optionalProperties_with_types")

        nodeIds.map { idAny =>
          val id = idAny.toString.toLong
          val propMap = nodeIdToProps.getOrElse(id, Map.empty)

          val children = propsWithTypes.map { p =>
            val Array(name, _) = p.split(":", 2)
            val value = propMap.getOrElse(name, "")
            Elem(null, name, Null, TopScope, true, Text(value))
          } ++ Seq(
            Elem(null, "id", Null, TopScope, true, Text(id.toString)),
            Elem(null, "original_label", Null, TopScope, true, Text(nodeType.name))
          )

          Elem(null, nodeType.name, Null, TopScope, true, children: _*)
        }
      }

      Elem(null, s"${nodeType.name}Group", scala.xml.Attribute(null, "type", "node", Null), TopScope, true, nodes: _*)
    }

    val groupedEdgeElems = complexTypes.filter(_.kind == "edge").map { edgeType =>
      val matchingDF = mergedEdges.filter(array_contains(col("relationshipTypes"), edgeType.name))
      val edges = matchingDF.collect().flatMap { row =>
        val edgeIds = row.getAs[Seq[Row]]("edgeIdsInCluster")

        edgeIds.map { eid =>
          val src = eid.getAs[Any]("srcId").toString.toLong
          val dst = eid.getAs[Any]("dstId").toString.toLong
          val propMap = edgeIdToSrcDst.getOrElse((src, dst), Map.empty)

          val props = edgeType.fields.map { case (name, _) =>
            val value = propMap.getOrElse(name, "")
            Elem(null, name, Null, TopScope, true, Text(value))
          }

          val srcType = propMap("srcType")
          val dstType = propMap("dstType")

          val children = Seq(
            Elem(null, "source", Null, TopScope, true,
              Elem(null, srcType, Null, TopScope, true, Text(src.toString))
            ),
            Elem(null, "target", Null, TopScope, true,
              Elem(null, dstType, Null, TopScope, true, Text(dst.toString))
            )
          ) ++ props

          Elem(null, edgeType.name, Null, TopScope, true, children: _*)
        }
      }

      Elem(null, s"${edgeType.name}Group", scala.xml.Attribute(null, "type", "edge", Null), TopScope, true, edges: _*)
    }

    val finalXml = Elem(null, "Graph", Null, TopScope, true, (groupedNodeElems ++ groupedEdgeElems): _*)
    val pp = new PrettyPrinter(120, 2)
    val prettyXml = pp.format(finalXml)
    val writer = new java.io.PrintWriter(outputPath, "UTF-8")
    writer.println("""<?xml version="1.0" encoding="UTF-8"?>""")
    writer.println(prettyXml)
    writer.close()
    println(s"[DONE] Exported XML to $outputPath")
  }
}
