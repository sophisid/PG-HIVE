import scala.xml._
import java.io.PrintWriter

case class ComplexType(name: String, kind: String, fields: Seq[(String, String)])

object XSD2X3MLGenerator {

  def parseXSD(path: String): Seq[ComplexType] = {
    val xsd = XML.loadFile(path)
    (xsd \ "complexType").map { ct =>
      val name = (ct \ "@name").text
      val kind = {
        val appinfoText = (ct \ "annotation" \ "appinfo").text +
                          (ct \ "sequence" \ "annotation" \ "appinfo").text
        if (appinfoText.contains("node")) "node"
        else if (appinfoText.contains("edge")) "edge"
        else "unknown"
      }
      val fields = (ct \ "sequence" \ "element").map { el =>
        val fname = (el \ "@name").text
        val ftype = (el \ "@type").text
        (fname, ftype)
      }
      ComplexType(name, kind, fields)
    }
  }

  def generateX3ML(xsdPath: String, outputPath: String): Unit = {
    val types = parseXSD(xsdPath)

    val ns =
      <namespaces>
        <namespace prefix="xsd" uri="http://www.w3.org/2001/XMLSchema#"/>
        <namespace prefix="rdfs" uri="http://www.w3.org/2000/01/rdf-schema#"/>
        <namespace prefix="custom" uri="https://your-namespace/custom/"/>
        <namespace prefix="your" uri="https://your-namespace/"/>
      </namespaces>

    val mappings = types.flatMap {
      case ct if ct.kind == "node" =>
        val domain =
          <domain>
            <source_node>//{ct.name}</source_node>
            <target_node>
              <entity>
                <type>custom:{ct.name}</type>
                <instance_generator name="URIwithType">
                  <arg name="id" type="xpath">id/text()</arg>
                  <arg name="type" type="constant">{ct.name}</arg>
                </instance_generator>
                <label_generator name="Label">
                  <arg name="label" type="xpath">id/text()</arg>
                </label_generator>
              </entity>
            </target_node>
          </domain>

        val links = ct.fields.map { case (fname, _) =>
          <link>
            <path>
              <source_relation>
                <relation>./{fname}</relation>
              </source_relation>
              <target_relation>
                <relationship>custom:has_{fname}</relationship>
              </target_relation>
            </path>
            <range>
              <source_node>./{fname}</source_node>
              <target_node>
                <entity>
                  <type>rdfs:Literal</type>
                  <instance_generator name="Literal">
                    <arg name="text" type="xpath">text()</arg>
                  </instance_generator>
                </entity>
              </target_node>
            </range>
          </link>
        }

        Seq(<mapping namedgraph="custom">{domain ++ links}</mapping>)

      case ct if ct.kind == "edge" =>
        val sourceTypeOpt = ct.fields.find(_._1 == "source").map(_._2).getOrElse("UnknownSource")
        val targetTypeOpt = ct.fields.find(_._1 == "target").map(_._2).getOrElse("UnknownTarget")

        val domain =
          <domain>
            <source_node>//{ct.name}/source/{sourceTypeOpt}</source_node>
            <target_node>
              <entity>
                <type>custom:{sourceTypeOpt}</type>
                <instance_generator name="URIwithType">
                  <arg name="id" type="xpath">text()</arg>
                  <arg name="type" type="constant">{sourceTypeOpt}</arg>
                </instance_generator>
              </entity>
            </target_node>
          </domain>

        val mainLink =
          <link>
            <path>
              <source_relation>
                <relation>../../target/{targetTypeOpt}</relation>
              </source_relation>
              <target_relation>
                <relationship>custom:{ct.name}</relationship>
              </target_relation>
            </path>
            <range>
              <source_node>../../target/{targetTypeOpt}</source_node>
              <target_node>
                <entity>
                  <type>custom:{targetTypeOpt}</type>
                  <instance_generator name="URIwithType">
                    <arg name="id" type="xpath">text()</arg>
                    <arg name="type" type="constant">{targetTypeOpt}</arg>
                  </instance_generator>
                </entity>
              </target_node>
            </range>
          </link>

        val edgePropertyLinks = ct.fields.collect {
          case (fname, _) if fname != "source" && fname != "target" =>
            <link>
              <path>
                <source_relation>
                  <relation>../../{fname}</relation>
                </source_relation>
                <target_relation>
                  <relationship>custom:{ct.name}_{fname}</relationship>
                </target_relation>
              </path>
              <range>
                <source_node>../../{fname}</source_node>
                <target_node>
                  <entity>
                    <type>rdfs:Literal</type>
                    <instance_generator name="Literal">
                      <arg name="text" type="xpath">text()</arg>
                    </instance_generator>
                  </entity>
                </target_node>
              </range>
            </link>
        }

        Seq(<mapping namedgraph="custom">{domain ++ mainLink ++ edgePropertyLinks}</mapping>)

      case _ => Nil
    }

    val x3ml =
      <x3ml source_type="xpath" version="1.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
            xsi:noNamespaceSchemaLocation="x3ml_v1.0.xsd">
        {ns}
        <mappings>{mappings}</mappings>
      </x3ml>

    val pp = new PrettyPrinter(120, 2)
    val out = new PrintWriter(outputPath)
    out.println("""<?xml version="1.0" encoding="UTF-8"?>""")
    out.println(pp.format(x3ml))
    out.close()
    println(s"[DONE] X3ML generated to $outputPath")
  }

}
