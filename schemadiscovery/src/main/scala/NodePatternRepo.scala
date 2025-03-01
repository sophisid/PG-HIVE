import scala.collection.mutable

object NodePatternRepository {

  val allPatterns: mutable.ArrayBuffer[NodePattern] = mutable.ArrayBuffer.empty

  def findMatchingPattern(label: String, properties: Set[String]): Option[NodePattern] = {
    allPatterns.find { p =>
      p.label == label && p.properties == properties
    }
  }

  def createPattern(label: String, properties: Set[String], nodeId: Long): NodePattern = {
    val newPatternId = allPatterns.size.toLong + 1
    val newPattern = NodePattern(newPatternId, label, properties, mutable.Set(nodeId))
    allPatterns += newPattern
    newPattern
  }
}
