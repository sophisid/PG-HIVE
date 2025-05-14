import org.apache.spark.sql.DataFrame
import java.io._

object PGSchemaExporterLoose {

  def exportPGSchema(nodesDF: DataFrame, edgesDF: DataFrame, outputPath: String): Unit = {
    val writer = new PrintWriter(new File(outputPath))
    writer.println("CREATE GRAPH TYPE NewGraphSchema LOOSE {")

    val ignoreProps = Set("original_label", "labelArray", "labelVector", "features", "prop_original_label")

    val edgeRows = edgesDF.collect()
    val hasEmptySrc = edgeRows.exists { row =>
      val srcLabels = row.getAs[Seq[String]]("srcLabels")
      srcLabels.isEmpty || srcLabels == Seq("") || srcLabels.forall(_.trim.isEmpty)
    }
    val hasEmptyDst = edgeRows.exists { row =>
      val dstLabels = row.getAs[Seq[String]]("dstLabels")
      dstLabels.isEmpty || dstLabels == Seq("") || dstLabels.forall(_.trim.isEmpty)
    }

    // --- NODE TYPES ---
    nodesDF.collect().foreach { row =>
      val labels = row.getAs[Seq[String]]("sortedLabels")
      val baseLabel = if (labels.nonEmpty) {
        if (labels.size == 1) labels.head
        else labels.mkString(" | ")
      } else "Abstract"
      val typeName = if (labels.nonEmpty) labels.mkString("_") + "Type" else "AbstractType"

      val allProps = (
        Option(row.getAs[Seq[String]]("mandatoryProperties_with_types")).getOrElse(Seq.empty) ++
        Option(row.getAs[Seq[String]]("optionalProperties_with_types")).getOrElse(Seq.empty)
      ).filterNot(p => ignoreProps.exists(p.toLowerCase.contains))
        .map(_.split(":")).collect {
          case Array(name, dtype) => s"${name.trim} ${normalizeType(dtype)}"
        }

      if (allProps.nonEmpty)
        writer.println(s"  ($typeName: $baseLabel {${allProps.mkString(", ")}}),")
      else
        writer.println(s"  ($typeName: $baseLabel),")
    }

    if (hasEmptySrc) writer.println(s"  (ABSTRACT_SRC: OPEN),")
    if (hasEmptyDst) writer.println(s"  (ABSTRACT_DST: OPEN),")

    // --- EDGE TYPES ---
    edgeRows.foreach { row =>
      val relTypes = row.getAs[Seq[String]]("relationshipTypes")
      val srcLabels = row.getAs[Seq[String]]("srcLabels")
      val dstLabels = row.getAs[Seq[String]]("dstLabels")

      val allProps = (
        Option(row.getAs[Seq[String]]("mandatoryProperties_with_types")).getOrElse(Seq.empty) ++
        Option(row.getAs[Seq[String]]("optionalProperties_with_types")).getOrElse(Seq.empty)
      ).filterNot(p => ignoreProps.exists(p.toLowerCase.contains))
        .map(_.split(":")).collect {
          case Array(name, dtype) => s"${name.trim} ${normalizeType(dtype)}"
        }

      val propStr = if (allProps.nonEmpty) s" {${allProps.mkString(", ")}}" else ""

      val relName = relTypes.mkString("_") + "Type"
      val relLabel = relTypes.mkString(" | ")
      val src = if (srcLabels.isEmpty || srcLabels == Seq("") || srcLabels.forall(_.trim.isEmpty)) "ABSTRACT_SRC" else srcLabels.map(_ + "Type").mkString("|")
      val dst = if (dstLabels.isEmpty || dstLabels == Seq("") || dstLabels.forall(_.trim.isEmpty)) "ABSTRACT_DST" else dstLabels.map(_ + "Type").mkString("|")

      writer.println(s"  (:$src)-[$relName: $relLabel$propStr]->(:$dst),")
    }

    writer.println("}")
    writer.close()
    println(s"PG LOOSE Schema has been successfully exported to $outputPath")
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
      case other        => "STRING" // Default to STRING for unknown types
    }
  }
}