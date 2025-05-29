import java.io.{File, PrintWriter}
import scala.xml._
import org.apache.spark.sql.DataFrame

object XSDExporter {

  def exportXSD(finalNodePatterns: DataFrame, finalEdgePatterns: DataFrame, outputPath: String): Unit = {

    var unknownCounter = 1
    var abstractSrcCounter = 1
    var abstractDstCounter = 1


    val nodeTypes = finalNodePatterns.collect().map { row =>
      val labels = row.getAs[Seq[String]]("sortedLabels")
      val mandatoryPropsWithTypes = Option(row.getAs[Seq[String]]("mandatoryProperties_with_types")).getOrElse(Seq.empty)
      val optionalPropsWithTypes = Option(row.getAs[Seq[String]]("optionalProperties_with_types")).getOrElse(Seq.empty)

      val nodeTypeName = if (labels.isEmpty) {
        val name = s"UNKNOWN_$unknownCounter"
        unknownCounter += 1
        name
      } else {
        labels.head
      }

      val props =
        (mandatoryPropsWithTypes ++ optionalPropsWithTypes)
          .filterNot(prop => prop.toLowerCase.contains("original_label") || prop.toLowerCase.contains("original_data"))
          .flatMap { propWithType =>
            val parts = propWithType.split(":").map(_.trim)
            if (parts.length == 2) {
              val (name, dtypeRaw) = (parts(0), parts(1))
              val minOccurs = if (mandatoryPropsWithTypes.exists(_.startsWith(name))) "1" else "0"
              val xsdType = mapToXSDType(dtypeRaw)
              Some(<xs:element name={name} type={xsdType} minOccurs={minOccurs} maxOccurs="1"/>)
            } else None
          }

      val sequence = if (props.nonEmpty) <xs:sequence>{props}</xs:sequence> else NodeSeq.Empty

      <xs:complexType name={nodeTypeName}>
        <xs:annotation>
          <xs:appinfo>
            <type>node</type>
          </xs:appinfo>
        </xs:annotation>
        {sequence}
        <xs:attribute name="id" type="xs:ID" use="required"/>
        <xs:attribute name="label" type="xs:string"/>
      </xs:complexType>
    }

    val groupedEdges = finalEdgePatterns.collect().groupBy { row =>
      row.getAs[Seq[String]]("relationshipTypes").headOption.getOrElse("UnknownEdge")
    }

    val edgeTypes = groupedEdges.map { case (relType, patterns) =>
      val sourceLabels = patterns.flatMap(r => Option(r.getAs[Seq[String]]("srcLabels")).getOrElse(Seq.empty)).toSet
      val targetLabels = patterns.flatMap(r => Option(r.getAs[Seq[String]]("dstLabels")).getOrElse(Seq.empty)).toSet

      val mandatoryPropsWithTypes = patterns.flatMap(r => Option(r.getAs[Seq[String]]("mandatoryProperties_with_types")).getOrElse(Seq.empty))
      val optionalPropsWithTypes = patterns.flatMap(r => Option(r.getAs[Seq[String]]("optionalProperties_with_types")).getOrElse(Seq.empty))




      val sourceElements = if (sourceLabels.isEmpty || sourceLabels.forall(_.trim.isEmpty)) {
        val abstractSrcType = s"ABSTRACT_SRC${abstractSrcCounter}"
        abstractSrcCounter += 1
        Seq(<xs:element name="source" type={abstractSrcType}/>)
      } else {
        sourceLabels.filter(_.trim.nonEmpty).map { src =>
          <xs:element name="source" type={src}/>
        }
      }


      val targetElements = if (targetLabels.isEmpty || targetLabels.forall(_.trim.isEmpty)) {
        val abstractDstType = s"ABSTRACT_DST${abstractDstCounter}"
        abstractDstCounter += 1
        Seq(<xs:element name="target" type={abstractDstType}/>)
      } else {
        targetLabels.filter(_.trim.nonEmpty).map { tgt =>
          <xs:element name="target" type={tgt}/>
        }
      }



      val propElements = (mandatoryPropsWithTypes ++ optionalPropsWithTypes).flatMap { propWithType =>
        val parts = propWithType.split(":").map(_.trim)
        if (parts.length == 2) {
          val (name, dtypeRaw) = (parts(0), parts(1))
          val minOccurs = if (mandatoryPropsWithTypes.exists(_.startsWith(name))) "1" else "0"
          val xsdType = mapToXSDType(dtypeRaw)
          Some(<xs:element name={name} type={xsdType} minOccurs={minOccurs} maxOccurs="1"/>)
        } else None
      }

      <xs:complexType name={relType}>
        <xs:sequence>
           <xs:annotation>
            <xs:appinfo>
              <type>edge</type>
            </xs:appinfo>
          </xs:annotation>
          {sourceElements}
          {targetElements}
          {propElements}
        </xs:sequence>
      </xs:complexType>
    }

    val schema =
      <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema" name="NewGraphSchema">
        {nodeTypes}
        {edgeTypes}
      </xs:schema>

    val writer = new PrintWriter(new File(outputPath))
    writer.write(new PrettyPrinter(80, 2).format(schema))
    writer.close()

    println(s"XSD Schema generated successfully at '$outputPath'!")
  }

  def mapToXSDType(scalaType: String): String = {
    scalaType.trim.toLowerCase match {
      case "string" => "xs:string"
      case "int" | "integer" | "int32" => "xs:integer"
      case "double" => "xs:double"
      case "boolean" => "xs:boolean"
      case "date" => "xs:date"
      case _ => "xs:string"
    }
  }
}
