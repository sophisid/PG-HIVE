import scala.collection.mutable

case class NodePattern(
  patternId: Long,
  label: String,
  properties: Set[String],
  assignedNodes: mutable.Set[Long] = mutable.Set.empty[Long]
)
