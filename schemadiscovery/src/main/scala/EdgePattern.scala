import scala.collection.mutable

case class EdgePattern(
  patternId: Long,
  relationshipType: Set[String],
  srcLabels: Set[String], // Πολλαπλά srcLabels
  dstLabels: Set[String], // Πολλαπλά dstLabels
  properties: Set[String],
  assignedEdges: mutable.Map[Long, String] = mutable.Map.empty[Long, String]
)
{
  def assignedEdgeIds: Set[Long] = assignedEdges.keys.toSet
}