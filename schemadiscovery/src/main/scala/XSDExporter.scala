import java.io.{File, PrintWriter}
import scala.xml._
import org.apache.spark.sql.DataFrame

object XSDExporter {

  def exportXSD(finalNodePatterns: DataFrame, finalEdgePatterns: DataFrame, outputPath: String): Unit = {

    val nodeTypes = finalNodePatterns.collect().map { row =>
      val nodeType = row.getAs[Seq[String]]("sortedLabels").head
      val mandatoryProps = row.getAs[Seq[String]]("mandatoryProperties")
      val optionalProps = row.getAs[Seq[String]]("optionalProperties")

      val props = 
        (mandatoryProps.map { prop =>
          <xs:element name={prop} type="xs:string" minOccurs="1" maxOccurs="1"/>
        } ++
        optionalProps.map { prop =>
          <xs:element name={prop} type="xs:string" minOccurs="0" maxOccurs="1"/>
        })

      val sequence = if (props.nonEmpty) <xs:sequence>{props}</xs:sequence> else NodeSeq.Empty

      <xs:complexType name={nodeType}>
        {sequence}
        <xs:attribute name="id" type="xs:ID" use="required"/>
        <xs:attribute name="label" type="xs:string"/>
      </xs:complexType>
    }

    val groupedEdges = finalEdgePatterns.collect().groupBy { row =>
      row.getAs[Seq[String]]("relationshipTypes").head
    }

    val edgeTypes = groupedEdges.map { case (relType, patterns) =>
      val sourceLabels = patterns.flatMap(_.getAs[Seq[String]]("srcLabels")).toSet
      val targetLabels = patterns.flatMap(_.getAs[Seq[String]]("dstLabels")).toSet
      val properties = patterns.flatMap(_.getAs[Seq[String]]("mandatoryProperties")).toSet

      val sourceElements = sourceLabels.map { src =>
        <xs:element name="source" type={src}/>
      }
      val targetElements = targetLabels.map { tgt =>
        <xs:element name="target" type={tgt}/>
      }

      val sourceSeq =
        if (sourceElements.size > 1) <xs:choice minOccurs="1">{sourceElements}</xs:choice>
        else sourceElements

      val targetSeq =
        if (targetElements.size > 1) <xs:choice minOccurs="1">{targetElements}</xs:choice>
        else targetElements

      val propElements = properties.map { prop =>
        <xs:element name={prop} type="xs:string" minOccurs="0" maxOccurs="1"/>
      }

      <xs:complexType name={relType}>
        <xs:sequence>
          {sourceSeq}
          {targetSeq}
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

    println(s"✅ XSD Schema generated successfully at '$outputPath'!")
  }
}
