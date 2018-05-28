package ch.epfl.bluebrain.nexus.sourcing.akka

/**
  * Wrapper class that represents a persistence id, with an optional qualifier (such as a Cassandra keyspace).
  */
final case class PersistenceId(value: String, qualifier: Option[String]) {
  override def toString: String = qualifier match {
    case None         => value
    case Some(qualif) => s"$qualif:$value"
  }
}

object PersistenceId {
  def apply(value: String): PersistenceId = PersistenceId(value, None)

  def apply(value: String, qualifier: String): PersistenceId = PersistenceId(value, Some(qualifier))
}
