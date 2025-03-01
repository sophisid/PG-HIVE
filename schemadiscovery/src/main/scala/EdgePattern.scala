import scala.collection.mutable

case class EdgePattern(
  patternId: Long,
  relationshipType: String,
  srcLabel: String,
  dstLabel: String,
  properties: Set[String],
  assignedEdges: mutable.Set[Long] = mutable.Set.empty[Long]
)
