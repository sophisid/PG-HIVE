import scala.collection.mutable

case class NodePattern(patternId: Long, label: String, properties: Set[String], assignedNodes: mutable.ArrayBuffer[Long])
case class EdgePattern(patternId: Long, label: String, srcLabel: String, dstLabel: String, properties: Set[String], assignedEdges: mutable.ArrayBuffer[Long])

object NodePatternRepository {
  private val patterns = mutable.ArrayBuffer[NodePattern]()
  private var nextId = 1L

  def findMatchingPattern(label: String, props: Set[String]): Option[NodePattern] = {
    patterns.find(p => p.label == label && p.properties == props)
  }

  def createPattern(label: String, props: Set[String], nodeId: Long): NodePattern = {
    val p = NodePattern(nextId, label, props, mutable.ArrayBuffer(nodeId))
    nextId += 1
    patterns += p
    p
  }

  def allPatterns: Seq[NodePattern] = patterns.toSeq
}

object EdgePatternRepository {
  private val patterns = mutable.ArrayBuffer[EdgePattern]()
  private var nextId = 1L

  def findMatchingPattern(label: String, srcLabel: String, dstLabel: String, props: Set[String]): Option[EdgePattern] = {
    patterns.find(p => p.label == label && p.srcLabel == srcLabel && p.dstLabel == dstLabel && p.properties == props)
  }

  def createPattern(label: String, srcLabel: String, dstLabel: String, props: Set[String], edgeId: Long): EdgePattern = {
    val p = EdgePattern(nextId, label, srcLabel, dstLabel, props, mutable.ArrayBuffer(edgeId))
    nextId += 1
    patterns += p
    p
  }

  def allPatterns: Seq[EdgePattern] = patterns.toSeq
}
