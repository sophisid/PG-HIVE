import org.apache.spark.sql.DataFrame
import java.io._

object PGSchemaExporterStrict {

  def exportPGSchema(nodesDF: DataFrame, edgesDF: DataFrame, outputPath: String): Unit = {
    val writer = new PrintWriter(new File(outputPath))

    val ignoreProps = Set("original_label", "labelArray", "labelVector", "features", "prop_original_label")

    // === NODE TYPE DEFINITIONS ===
    writer.println("-- NODE TYPES --")
    var abstractNodeCounter = 1

    val nodeTypeNames = nodesDF.collect().map { row =>
      val labels = row.getAs[Seq[String]]("sortedLabels")
      val (typeName, baseLabel) = if (labels.isEmpty) {
        val absName = s"AbstractType_$abstractNodeCounter"
        val absLabel = s"ABSTRACT_$abstractNodeCounter"
        abstractNodeCounter += 1
        (absName, absLabel)
      } else {
        val typeN = labels.mkString("_") + "Type"
        val baseL = if (labels.size > 1) labels.mkString(" | ") else labels.last
        (typeN, baseL)
      }

      val mandatoryProps = Option(row.getAs[Seq[String]]("mandatoryProperties_with_types")).getOrElse(Seq.empty)
        .filterNot(p => ignoreProps.exists(p.toLowerCase.contains))
        .map(_.split(":")).collect {
          case Array(name, dtype) => s"${name.trim} ${normalizeType(dtype)}"
        }

      val optionalProps = Option(row.getAs[Seq[String]]("optionalProperties_with_types")).getOrElse(Seq.empty)
        .filterNot(p => ignoreProps.exists(p.toLowerCase.contains))
        .map(_.split(":")).collect {
          case Array(name, dtype) => s"OPTIONAL ${name.trim} ${normalizeType(dtype)}"
        }

      val allProps = mandatoryProps ++ optionalProps

      if (allProps.nonEmpty)
        writer.println(s"CREATE NODE TYPE $typeName : $baseLabel {${allProps.mkString(", ")}};")
      else
        writer.println(s"CREATE NODE TYPE $typeName : $baseLabel;")

      typeName
    }.toBuffer

    // === Check if ABSTRACT_SRC / ABSTRACT_DST are needed ===
    val edgeRows = edgesDF.collect()
    val hasEmptySrc = edgeRows.exists { row =>
      val srcLabels = row.getAs[Seq[String]]("srcLabels")
       srcLabels.isEmpty || srcLabels == Seq("") || srcLabels.forall(_.trim.isEmpty)
    }
    val hasEmptyDst = edgeRows.exists { row =>
      val dstLabels = row.getAs[Seq[String]]("dstLabels")
       dstLabels.isEmpty || dstLabels == Seq("") || dstLabels.forall(_.trim.isEmpty)
    }

    if (hasEmptySrc) writer.println(s"CREATE NODE TYPE ABSTRACT_SRC : OPEN;")
    if (hasEmptyDst) writer.println(s"CREATE NODE TYPE ABSTRACT_DST : OPEN;")

    writer.println("\n-- EDGE TYPES --")
    val edgeTypeNames = edgeRows.map { row =>
      val relTypes = row.getAs[Seq[String]]("relationshipTypes")
      val (relName, edgeLabel) = if (relTypes.isEmpty) {
        ("AbstractEdgeType", "ABSTRACT_EDGE")
      } else {
        (relTypes.mkString("_").toLowerCase + "Type", relTypes.mkString(" | "))
      }

      val mandatoryProps = Option(row.getAs[Seq[String]]("mandatoryProperties_with_types")).getOrElse(Seq.empty)
        .filterNot(p => ignoreProps.exists(p.toLowerCase.contains))
        .map(_.split(":")).collect {
          case Array(name, dtype) => s"${name.trim} ${normalizeType(dtype)}"
        }

      val optionalProps = Option(row.getAs[Seq[String]]("optionalProperties_with_types")).getOrElse(Seq.empty)
        .filterNot(p => ignoreProps.exists(p.toLowerCase.contains))
        .map(_.split(":")).collect {
          case Array(name, dtype) => s"OPTIONAL ${name.trim} ${normalizeType(dtype)}"
        }

      val allProps = mandatoryProps ++ optionalProps
      val propStr = if (allProps.nonEmpty) s" {${allProps.mkString(", ")}}" else ""

      writer.println(s"CREATE EDGE TYPE $relName : $edgeLabel$propStr;")

      (relName, row)
    }

    writer.println("\nCREATE GRAPH TYPE NewGraphSchema STRICT {")

    nodeTypeNames.foreach { typeName =>
      writer.println(s"  ($typeName),")
    }
    if (hasEmptySrc) writer.println(s"  (ABSTRACT_SRC),")
    if (hasEmptyDst) writer.println(s"  (ABSTRACT_DST),")

    edgeTypeNames.foreach { case (relName, row) =>
      val srcLabels = row.getAs[Seq[String]]("srcLabels")
      val dstLabels = row.getAs[Seq[String]]("dstLabels")

      val srcType = if ( srcLabels.isEmpty || srcLabels == Seq("") || srcLabels.forall(_.trim.isEmpty)) "ABSTRACT_SRC" else srcLabels.map(_ + "Type").mkString("|")
      val dstType = if ( dstLabels.isEmpty || dstLabels == Seq("") || dstLabels.forall(_.trim.isEmpty)) "ABSTRACT_DST" else dstLabels.map(_ + "Type").mkString("|")

      writer.println(s"  (:$srcType)-[$relName]->(:$dstType),")
    }

    writer.println("\n  // Constraints")

    edgeTypeNames.foreach { case (relName, row) =>
      val srcLabels = row.getAs[Seq[String]]("srcLabels")
      val dstLabels = row.getAs[Seq[String]]("dstLabels")

      val srcType = if ( srcLabels.isEmpty || srcLabels == Seq("") || srcLabels.forall(_.trim.isEmpty)) "ABSTRACT_SRC" else srcLabels.filter(_.nonEmpty).map(_ + "Type").mkString("|")
      val dstType = if ( dstLabels.isEmpty || dstLabels == Seq("") || dstLabels.forall(_.trim.isEmpty)) "ABSTRACT_DST" else dstLabels.filter(_.nonEmpty).map(_ + "Type").mkString("|")

      val cardinality = row.getAs[String]("cardinality")

      cardinality match {
        case "1:1" =>
          writer.println(s"  FOR (x:$srcType) SINGLETON x,y WITHIN (x)-[y: $relName]->(:$dstType)")
        case "N:1" =>
          writer.println(s"  FOR (x:$srcType) SINGLETON y WITHIN (x)-[y: $relName]->(:$dstType)")
        case "1:N" =>
          writer.println(s"  FOR (x:$dstType) SINGLETON x WITHIN (:$srcType)-[y: $relName]->(x)")
        case _ => // N:N or unknown -> no constraint
      }
    }

    writer.println("}")
    writer.close()
    println(s"PG STRICT Schema with constraints has been successfully exported to $outputPath")
  }

  def normalizeType(dt: String): String = {
    dt.trim.toLowerCase match {
      case "string"     => "STRING"
      case "int"        => "INT"
      case "int32"      => "INT32"
      case "integer"    => "INTEGER"
      case "date"       => "DATE"
      case "double"     => "DOUBLE"
      case "boolean"    => "BOOLEAN"
      case other         => "STRING" // Default to STRING for unknown types
    }
  }
}