import scala.collection.mutable

object EdgePatternRepository {
  val allPatterns: mutable.ArrayBuffer[EdgePattern] = mutable.ArrayBuffer.empty

  def findMatchingPattern(relationshipType: Set[String],
                          srcLabels: Set[String],
                          dstLabels: Set[String],
                          props: Set[String]): Option[EdgePattern] = {
    allPatterns.find { p =>
      p.relationshipType == relationshipType &&
      p.srcLabels == srcLabels &&
      p.dstLabels == dstLabels &&
      p.properties == props
    }
  }

  def createPattern(relationshipType: Set[String],
                    srcLabels: Set[String],
                    dstLabels: Set[String],
                    props: Set[String],
                    edgeId: Long,
                    initialLabel: String): EdgePattern = {
    val newPatternId = allPatterns.size.toLong + 1
    val newPattern = EdgePattern(newPatternId, relationshipType, srcLabels, dstLabels, props, mutable.Map(edgeId -> initialLabel))
    allPatterns += newPattern
    newPattern
  }
}