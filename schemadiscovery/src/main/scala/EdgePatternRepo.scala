import scala.collection.mutable

object EdgePatternRepository {
  val allPatterns: mutable.ArrayBuffer[EdgePattern] = mutable.ArrayBuffer.empty

  def findMatchingPattern(relationshipType: String,
                          srcLabel: String,
                          dstLabel: String,
                          props: Set[String]): Option[EdgePattern] = {
    allPatterns.find { p =>
      p.relationshipType == relationshipType &&
      p.srcLabel == srcLabel &&
      p.dstLabel == dstLabel &&
      p.properties == props
    }
  }

  def createPattern(relationshipType: String,
                    srcLabel: String,
                    dstLabel: String,
                    props: Set[String],
                    edgeId: Long): EdgePattern = {
    val newPatternId = allPatterns.size.toLong + 1
    val newPattern = EdgePattern(newPatternId, relationshipType, srcLabel, dstLabel, props, mutable.Set(edgeId))
    allPatterns += newPattern
    newPattern
  }
}
