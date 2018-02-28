package ch.epfl.bluebrain.nexus.sourcing.akka.cache

@SuppressWarnings(Array("IncorrectlyNamedExceptions"))
sealed abstract class CacheError(message: String) extends Exception {
  override def fillInStackTrace(): CacheError = this
  override val getMessage: String             = message
}

@SuppressWarnings(Array("IncorrectlyNamedExceptions"))
object CacheError {
  final case object TypeError                  extends CacheError("Error on attempting to cast cache value")
  final case object EmptyKey                   extends CacheError("Empty key")
  final case object TimeoutError               extends CacheError("Timeout error while waiting for the actor to respond")
  final case class UnknownError(th: Throwable) extends CacheError("Unknown error")
  final case class UnexpectedReply(value: Any) extends CacheError("Unexpected reply from the actor")

}
