import scala.collection.mutable

case class NodePattern(
  patternId: Long,
  label: Set[String],
  properties: Set[String],
  assignedNodes: mutable.Map[Long, String] = mutable.Map.empty[Long, String]
)