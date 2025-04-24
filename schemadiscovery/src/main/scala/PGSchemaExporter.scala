import org.apache.spark.sql.DataFrame
import java.io._

object PGSchemaExporter {

  def exportPGSchema(nodesDF: DataFrame, edgesDF: DataFrame, outputPath: String): Unit = {
    val writer = new PrintWriter(new File(outputPath))
    writer.println("CREATE GRAPH TYPE NewGraphSchema LOOSE {")

    val ignoreProps = Set("original_label", "labelArray", "labelVector", "features", "prop_original_label")

    // --- NODE TYPES ---
    nodesDF.collect().foreach { row =>
      val labels = row.getAs[Seq[String]]("sortedLabels")
      val baseLabel = if (labels.nonEmpty) {
        if (labels.size == 1) labels.head
        else labels.mkString(" & ")
      } else "Abstract"
      val typeName = if (labels.nonEmpty) labels.mkString("_") + "Type" else "AbstractType"

      val allProps = (
        row.getAs[Seq[String]]("mandatoryProperties_with_types") ++
        row.getAs[Seq[String]]("optionalProperties_with_types")
      ).filterNot(p => ignoreProps.exists(p.toLowerCase.contains))
        .map(_.split(":")).collect {
          case Array(name, dtype) => s"${name.trim} ${normalizeType(dtype)}"
        }

      if (allProps.nonEmpty)
        writer.println(s"  ($typeName: $baseLabel {${allProps.mkString(", ")}}),")
      else
        writer.println(s"  ($typeName: $baseLabel),")
    }

    // --- EDGE TYPES ---
    edgesDF.collect().foreach { row =>
      val relTypes = row.getAs[Seq[String]]("relationshipTypes")
      val srcLabels = row.getAs[Seq[String]]("srcLabels")
      val dstLabels = row.getAs[Seq[String]]("dstLabels")

      val allProps = (
        row.getAs[Seq[String]]("mandatoryProperties_with_types") ++
        row.getAs[Seq[String]]("optionalProperties_with_types")
      ).filterNot(p => ignoreProps.exists(p.toLowerCase.contains))
        .map(_.split(":")).collect {
          case Array(name, dtype) => s"${name.trim} ${normalizeType(dtype)}"
        }

      val propStr = if (allProps.nonEmpty) s" {${allProps.mkString(", ")}}" else ""

      val relName = relTypes.mkString("_")
      val relLabel = relTypes.mkString(" & ")
      val src = srcLabels.mkString("|") + "Type"
      val dst = dstLabels.mkString("|") + "Type"

      writer.println(s"  (:$src)-[$relName: $relLabel$propStr]->(:$dst),")
    }

    writer.println("}")
    writer.close()
    println(s"✅ PG LOOSE Schema has been successfully exported to $outputPath")
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
    }
  }
}