package ch.epfl.bluebrain.nexus.sourcing.akka
import akka.routing.ConsistentHashingRouter.ConsistentHashable

/**
  * Enumeration that defines the message types exchanged with the underlying persistent actor.
  */
sealed trait Msg extends ConsistentHashable with Product with Serializable {

  /**
    * @return the persistence id
    */
  def id: String

  override def consistentHashKey: String = id
}

object Msg {

  /**
    * Message to trigger a new event to be appended to the event log.
    *
    * @param id  the persistence id
    * @param event the event to be appended
    * @tparam Event the type of the event
    */
  final case class Append[Event](id: String, event: Event) extends Msg

  /**
    * Message to confirm that a new event has been appended to the event log.
    *
    * @param id  the persistence id
    * @param lastSeqNr the sequence number of the appended event
    */
  final case class Appended(id: String, lastSeqNr: Long) extends Msg

  /**
    * Message to retrieve the current sequence number of an event log.
    *
    * @param id the persistence id
    */
  final case class GetLastSeqNr(id: String) extends Msg

  /**
    * Message for exchanging the last known sequence number of an event log.
    *
    * @param id  the persistence id
    * @param lastSeqNr the last sequence numbers
    */
  final case class LastSeqNr(id: String, lastSeqNr: Long) extends Msg

  /**
    * Message to retrieve the current state of a stateful event log.
    *
    * @param id the persistence id
    */
  final case class GetCurrentState(id: String) extends Msg

  /**
    * Message for exchanging the current state of a stateful event log.
    *
    * @param id    the persistence id
    * @param state the current state of the event log
    * @tparam State the type of the event log state
    */
  final case class CurrentState[State](id: String, state: State) extends Msg

  /**
    * Message to evaluate a command against an aggregate.
    *
    * @param id  the persistence id
    * @param cmd the command to evaluate
    * @tparam Command the type of the command to evaluate
    */
  final case class Evaluate[Command](id: String, cmd: Command) extends Msg

  /**
    * Message for replying with the outcome of evaluating a command against an aggregate.
    *
    * @param id    the persistence id
    * @param value either a rejection or the state derived from the last command evaluation
    * @tparam Rejection the type of rejection
    * @tparam State  the type of the event log state
    */
  final case class Evaluated[Rejection, State](id: String, value: Either[Rejection, State]) extends Msg

  /**
    * Message to check a command against an aggregate.
    *
    * @param id  the persistence id
    * @param cmd the command to check
    * @tparam Command the type of the command to check
    */
  final case class Test[Command](id: String, cmd: Command) extends Msg

  /**
    * Message for replying with the outcome of checking a command against an aggregate. The command will only be tested
    * and the result will be discarded. The current state of the aggregate will not change.
    *
    * @param id    the persistence id
    * @param value either a rejection or the state that would be produced from the command evaluation
    * @tparam Rejection the type of rejection
    */
  final case class Tested[Rejection, State](id: String, value: Either[Rejection, State]) extends Msg

  /**
    * Message to trigger a snapshot.
    *
    * @param id  the persistence id
    */
  final case class Snapshot(id: String) extends Msg

  /**
    * Message for replying with the successful outcome of saving a snapshot.
    *
    * @param id    the persistence id
    * @param seqNr the sequence number corresponding to the snapshot taken
    */
  final case class Snapshotted(id: String, seqNr: Long) extends Msg

  /**
    * Message for replying with a failed outcome of saving a snapshot.
    *
    * @param id the persistence id
    */
  final case class SnapshotFailed(id: String) extends Msg

  /**
    * Message for replying when an incorrect message is sent to the actor.
    *
    * @param id the persistence id
    */
  @SuppressWarnings(Array("IncorrectlyNamedExceptions"))
  final case class UnexpectedMsgId(id: String, receivedMsgId: String) extends Exception with Msg {
    override def fillInStackTrace(): Throwable = this
  }

  /**
    * Message for replying when an incorrect message value type is sent to the actor.
    *
    * @param id the persistence id
    * @param expected the expected message value type
    * @param received the received message value
    */
  @SuppressWarnings(Array("IncorrectlyNamedExceptions"))
  final case class TypeError(id: String, expected: String, received: Any) extends Exception with Msg {
    override def fillInStackTrace(): Throwable = this
  }

  /**
    * Message to signal that the actor should stop.
    *
    * @param id the persistence id
    */
  final case class Done(id: String) extends Msg

  /**
    * Message to signal that a command evaluation has timed out.
    *
    * @param id       the persistence id
    * @param command  the command that timed out
    * @tparam Command the type of the command
    */
  @SuppressWarnings(Array("IncorrectlyNamedExceptions"))
  final case class CommandEvaluationTimeout[Command](id: String, command: Command) extends Exception with Msg {
    override def fillInStackTrace(): Throwable = this
  }

  /**
    * Message to signal that a command evaluation has returned in an error.
    *
    * @param id       the persistence id
    * @param command  the command that timed out
    * @param message  an optional message describing the cause of the error
    * @tparam Command the type of the command
    */
  @SuppressWarnings(Array("IncorrectlyNamedExceptions"))
  final case class CommandEvaluationError[Command](id: String, command: Command, message: Option[String])
      extends Exception
      with Msg {
    override def fillInStackTrace(): Throwable = this
  }
}
