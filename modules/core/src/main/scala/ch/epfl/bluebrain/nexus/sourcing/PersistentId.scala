package ch.epfl.bluebrain.nexus.sourcing

/**
  * Wrapper class that represents a persistent id, within an optional keyspace
  */
final case class PersistentId(value: String, keyspace: Option[String]) {
  override def toString: String = keyspace match {
    case None     => value
    case Some(ks) => s"$ks:$value"
  }
}

object PersistentId {
  def apply(value: String): PersistentId = PersistentId(value, None)

  def apply(value: String, keyspace: String): PersistentId = PersistentId(value, Some(keyspace))
}
