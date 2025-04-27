import org.apache.spark.sql.DataFrame
import java.io._

object PGSchemaExporterStrict {

  def exportPGSchema(nodesDF: DataFrame, edgesDF: DataFrame, outputPath: String): Unit = {
    val writer = new PrintWriter(new File(outputPath))

    val ignoreProps = Set("original_label", "labelArray", "labelVector", "features", "prop_original_label")

    // === NODE TYPE DEFINITIONS ===
    writer.println("-- NODE TYPES --")
    nodesDF.collect().foreach { row =>
      val labels = row.getAs[Seq[String]]("sortedLabels")
      val typeName = labels.mkString("_") + "Type"
      val baseLabel = if (labels.size > 1) labels.mkString(" | ") else labels.lastOption.getOrElse("Unknown")

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
    }

    writer.println("\n-- EDGE TYPES --")
    edgesDF.collect().foreach { row =>
      val relTypes = row.getAs[Seq[String]]("relationshipTypes")
      val relName = relTypes.mkString("_").toLowerCase + "Type"
      val edgeLabel = relTypes.mkString(" | ")

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
    }

    writer.println("\nCREATE GRAPH TYPE NewGraphSchema STRICT {")

    // Reference node types
    nodesDF.collect().foreach { row =>
      val labels = row.getAs[Seq[String]]("sortedLabels")
      val typeName = labels.mkString("_") + "Type"
      writer.println(s"  ($typeName),")
    }

    // Reference edge types
    edgesDF.collect().foreach { row =>
      val relTypes = row.getAs[Seq[String]]("relationshipTypes")
      val relName = relTypes.mkString("_").toLowerCase + "Type"

      val src = row.getAs[Seq[String]]("srcLabels").map(_ + "Type").mkString("|")
      val dst = row.getAs[Seq[String]]("dstLabels").map(_ + "Type").mkString("|")

      writer.println(s"  (:$src)-[$relName]->(:$dst),")
    }

    writer.println("\n  // Constraints")

    // Constraints based on cardinalities
    edgesDF.collect().foreach { row =>
      val relTypes = row.getAs[Seq[String]]("relationshipTypes")
      val relName = relTypes.mkString("_").toLowerCase + "Type"

      val src = row.getAs[Seq[String]]("srcLabels").map(_ + "Type").mkString("|")
      val dst = row.getAs[Seq[String]]("dstLabels").map(_ + "Type").mkString("|")

      val cardinality = row.getAs[String]("cardinality")

      cardinality match {
        case "1:1" | "1:N" =>
          writer.println(s"  FOR (x:$src) SINGLETON y WITHIN (x)-[y: $relName]->(:$dst)")
        case "N:1" =>
          writer.println(s"  FOR (y:$dst) SINGLETON x WITHIN (x)-[x: $relName]->(:$dst)")
        case _ => // N:N or unknown -> no constraint
      }
    }
    
    writer.println("}")
    writer.close()
    println(s"✅ PG STRICT Schema with constraints has been successfully exported to $outputPath")
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
      case other         => other.toUpperCase
    }
  }
}