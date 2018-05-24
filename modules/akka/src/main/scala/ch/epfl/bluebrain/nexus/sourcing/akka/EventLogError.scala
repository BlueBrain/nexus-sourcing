package ch.epfl.bluebrain.nexus.sourcing.akka

import ch.epfl.bluebrain.nexus.sourcing.PersistentId

/**
  * Top level error type definition for the akka persistence backed sourcing implementation.
  *
  * @param message a descriptive message of the error
  */
@SuppressWarnings(Array("IncorrectlyNamedExceptions"))
sealed abstract class EventLogError(message: String) extends Exception {
  override def fillInStackTrace(): EventLogError = this
  override val getMessage: String                = message
}

/**
  * Error definition to signal that a type mismatch (received / expected) has occurred for a persistent id.
  *
  * @param id       the persistent id for which the error occurred
  * @param expected the expected message type
  * @param received the received message
  */
@SuppressWarnings(Array("IncorrectlyNamedExceptions"))
final case class TypeError(id: PersistentId, expected: String, received: Any)
    extends EventLogError(s"Received an unexpected '$received', expected '$expected' type for action on '$id'")

/**
  * Signals a timeout while waiting for the argument action to be evaluated on the argument id.
  *
  * @param id     the persistent id for which the error occurred
  * @param action the action that was performed
  * @tparam A the type of the action performed
  */
@SuppressWarnings(Array("IncorrectlyNamedExceptions"))
final case class TimeoutError[A](id: PersistentId, action: A)
    extends EventLogError(s"Timed out while expecting reply for action on '$id'")

/**
  * Error definition to signal that the underlying persistent actor has sent an unexpected message for an intended
  * actions.
  *
  * @param id     the persistent id for which the error occurred
  * @param action the action that was performed
  * @param reply  the reply received
  * @tparam A the type of the action performed
  * @tparam B the type of the reply received
  */
@SuppressWarnings(Array("IncorrectlyNamedExceptions"))
final case class UnexpectedReply[A, B](id: PersistentId, action: A, reply: B)
    extends EventLogError(s"Received an unexpected reply for action on '$id")

/**
  * Error definition to signal an unknown error that occurred materialized as an exception during the processing of
  * an action against an event log.
  *
  * @param id     the persistent id for which the error occurred
  * @param action the action that was performed
  * @param th     the exception thrown
  * @tparam A the type of the action performed
  */
@SuppressWarnings(Array("IncorrectlyNamedExceptions"))
final case class UnknownError[A](id: PersistentId, action: A, th: Throwable)
    extends EventLogError(s"Unknown error occurred while executing action on '$id'")
