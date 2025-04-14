/* import java.io.{File, PrintWriter}
import scala.xml._

//TODO:: KANONIKA DEN PREPEI NA EINAI OLA REQUIRED 8A PREPEI NA VLEPW AN KATI EIANI OPTIONAL H OXI 
object XSDExporter {

  def exportXSD(): Unit = {
    // Retrieve node and edge patterns from the repository
    val nodePatterns = NodePatternRepository.allPatterns
    val edgePatterns = EdgePatternRepository.allPatterns

    // Creating node types in the XSD schema
    val nodeTypes = nodePatterns.map { pattern =>
      val properties = pattern.properties.map(prop => <xs:element name={prop} type="xs:string" minOccurs="0"/>)
      val sequence = if (properties.nonEmpty) <xs:sequence>{properties}</xs:sequence> else NodeSeq.Empty

      <xs:complexType name={pattern.label.head}>
        {sequence}
        <xs:attribute name="id" type="xs:ID" use="required"/>
        <xs:attribute name="label" type="xs:string"/>
      </xs:complexType>
    }


    // Group edges by relationshipType to merge different target types as choices
    val edgeTypes = edgePatterns.groupBy(_.relationshipType.head).map { case (relType, patterns) =>
    val sourceChoices = patterns.flatMap(_.srcLabels).toSet.map { src: String =>
      <xs:element name="source" type={src}/>
    }
      val sourceSequence =
        if (sourceChoices.size > 1)
          <xs:choice minOccurs="1">{sourceChoices}</xs:choice>
        else
          sourceChoices

      val targetChoices = patterns.flatMap(_.dstLabels).toSet.map { target: String =>
        <xs:element name="target" type={target}/>
      }
      val targetSequence =
        if (targetChoices.size > 1)
          <xs:choice minOccurs="1">{targetChoices}</xs:choice>
        else
          targetChoices

      <xs:complexType name={relType}>
        <xs:sequence>
          {sourceSequence}
          {targetSequence}
        </xs:sequence>
      </xs:complexType>
    }


    // Creating the Graph Schema, listing all node and edge types
    val graphElement =
      <xs:element name="Graph">
        <xs:complexType>
          <xs:sequence>
            {nodePatterns.map(pattern => <xs:element name={pattern.label.head} type={pattern.label.head} maxOccurs="unbounded"/>)}
            {edgePatterns.map(pattern => <xs:element name={pattern.relationshipType.head} type={pattern.relationshipType.head} maxOccurs="unbounded"/>)}
          </xs:sequence>
        </xs:complexType>
      </xs:element>

    // Complete XSD Schema
    val schema =
      <xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
        {nodeTypes}
        {edgeTypes}
        {graphElement}
      </xs:schema>

    // Save the generated XSD schema to a file
    val writer = new PrintWriter(new File("schema_output.xsd"))
    writer.write(new PrettyPrinter(80, 2).format(schema))
    writer.close()

    println("✅ XSD Schema has been updated and generated correctly as 'schema_output.xsd'!")
  }
} */