import scala.collection.mutable

object NodePatternRepository {
  val allPatterns: mutable.ArrayBuffer[NodePattern] = mutable.ArrayBuffer.empty

  def findMatchingPattern(label: Set[String], props: Set[String]): Option[NodePattern] = {
    allPatterns.find { p =>
      p.label == label &&
      p.properties == props
    }
  }

  def createPattern(label: Set[String], props: Set[String], nodeId: Long, initialLabel: String): NodePattern = {
    val newPatternId = allPatterns.size.toLong + 1
    val newPattern = NodePattern(newPatternId, label, props, mutable.Map(nodeId -> initialLabel))
    allPatterns += newPattern
    newPattern
  }
}