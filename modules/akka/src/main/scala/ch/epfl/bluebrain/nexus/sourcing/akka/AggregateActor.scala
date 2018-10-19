package ch.epfl.bluebrain.nexus.sourcing.akka
import java.net.{URLDecoder, URLEncoder}
import java.util.concurrent.TimeoutException

import akka.actor.{Actor, ActorLogging, ActorRef, Props, ReceiveTimeout, Stash}
import akka.cluster.sharding.ShardRegion.Passivate
import akka.persistence._
import cats.effect.syntax.all._
import cats.effect.{ContextShift, Effect, IO, Timer}
import cats.implicits._
import ch.epfl.bluebrain.nexus.sourcing.akka.Msg._

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.control.NonFatal

/**
  * A persistent actor implementation that handles the role of an Aggregate: manages the event log, state transitions
  * and the command evaluation for a single entity.
  *
  * @param name                name of the aggregate (aggregates with the same name are part of the same group or share
  *                            the same "type")
  * @param initialState        the initial state of the aggregate
  * @param next                state transition function; represented as a total function without any effect types;
  *                            state transition functions should be pure
  * @param evaluate            command evaluation function; represented as a function that returns the evaluation in an
  *                            arbitrary effect type; may be asynchronous
  * @param passivationStrategy the strategy for gracefully passivating this actor
  * @param config              akka sourcing configuration
  * @tparam F                  the evaluation effect type
  * @tparam Event              the event type of the aggregate
  * @tparam State              the state type of the aggregate
  * @tparam Command            the command type of the aggregate
  * @tparam Rejection          the command evaluation rejection type of the aggregate
  */
//noinspection ActorMutableStateInspection
private[akka] abstract class AggregateActor[
    F[_]: Effect,
    Event: ClassTag,
    State: ClassTag,
    Command: ClassTag,
    Rejection: ClassTag
](
    name: String,
    initialState: State,
    next: (State, Event) => State,
    evaluate: (State, Command) => F[Either[Rejection, Event]],
    passivationStrategy: PassivationStrategy[State, Command],
    config: AkkaSourcingConfig,
) extends PersistentActor
    with Stash
    with ActorLogging {

  def id: String

  def passivate(): Unit

  override val persistenceId: String = s"$name-${URLEncoder.encode(id, "UTF-8")}"

  private val Event     = implicitly[ClassTag[Event]]
  private val State     = implicitly[ClassTag[State]]
  private val Command   = implicitly[ClassTag[Command]]
  private val Rejection = implicitly[ClassTag[Rejection]]

  private var state = initialState

  private implicit val timer: Timer[IO]     = IO.timer(config.commandEvalExecutionContext)
  private implicit val cs: ContextShift[IO] = IO.contextShift(config.commandEvalExecutionContext)

  override def preStart(): Unit = {
    super.preStart()
    log.debug("AggregateActor with id '{}' started", persistenceId)
    passivationStrategy.lapsedSinceLastInteraction.foreach { duration =>
      context.setReceiveTimeout(duration)
      log.debug("Configured actor with id '{}' to passivate after '{}' seconds of inactivity",
                persistenceId,
                duration.toSeconds)
    }
  }

  override def receiveRecover: Receive = {
    case RecoveryCompleted =>
      passivationStrategy.lapsedSinceRecoveryCompleted.foreach { duration =>
        context.system.scheduler.scheduleOnce(duration)(passivate())(context.dispatcher)
        log.debug("Configured actor with id '{}' to passivate after '{}' seconds", persistenceId, duration.toSeconds)
      }
      log.debug("Recovery completed on actor '{}'", persistenceId)
    case SnapshotOffer(metadata, State(st)) =>
      state = st
      log.debug("Applied snapshot '{}' with seq nr '{}' recovery on actor '{}'", st, metadata.sequenceNr, persistenceId)
    case Event(ev) =>
      state = next(state, ev)
      log.debug("Applied event '{}' to actor '{}'", ev, persistenceId)
    case other =>
      // $COVERAGE-OFF$
      log.error("Unknown message '{}' during recovery of actor '{}', expected message of type '{}'",
                other,
                persistenceId,
                Event.runtimeClass.getSimpleName)
    // $COVERAGE-ON$
  }

  override def receiveCommand: Receive = {
    case Append(mid, value) if mid == id =>
      value match {
        case Event(event) =>
          persist(event) { _ =>
            state = next(state, event)
            log.debug("Applied event '{}' to actor '{}'", event, persistenceId)
            sender() ! Appended(id, lastSequenceNr)
            passivateAfterEvaluation()
          }
        // $COVERAGE-OFF$
        case _ =>
          log.error("Received an event '{}' incompatible with the expected type '{}'",
                    value,
                    Event.runtimeClass.getSimpleName)
          sender() ! TypeError(id, Event.runtimeClass.getSimpleName, value)
          passivateAfterEvaluation()
        // $COVERAGE-ON$
      }
    case GetLastSeqNr(mid) if mid == id =>
      sender() ! LastSeqNr(id, lastSequenceNr)
      log.debug("Replied with LastSeqNr '{}' from actor '{}'", lastSequenceNr, persistenceId)
      passivateAfterEvaluation()
    case GetCurrentState(mid) if mid == id =>
      sender() ! CurrentState(id, state)
      log.debug("Replied with CurrentState '{}' from actor '{}'", state, persistenceId)
      passivateAfterEvaluation()
    case Evaluate(mid, value) if mid == id =>
      value match {
        case Command(cmd) =>
          log.debug("Evaluating command '{}' on actor '{}'", cmd, persistenceId)
          evaluateCommand(cmd)
          context.become(evaluating(cmd, sender()))
        // $COVERAGE-OFF$
        case _ =>
          log.error("Received a command '{}' incompatible with the expected type '{}'",
                    value,
                    Command.runtimeClass.getSimpleName)
          sender() ! TypeError(id, Command.runtimeClass.getSimpleName, value)
          passivateAfterEvaluation()
        // $COVERAGE-ON$
      }

    case Test(mid, value) if mid == id =>
      value match {
        case Command(cmd) =>
          log.debug("Testing command '{}' on actor '{}'", cmd, persistenceId)
          evaluateCommand(cmd, test = true)
          context.become(testing(cmd, sender()))
        // $COVERAGE-OFF$
        case _ =>
          log.error("Received a command '{}' incompatible with the expected type '{}'",
                    value,
                    Command.runtimeClass.getSimpleName)
          sender() ! TypeError(id, Command.runtimeClass.getSimpleName, value)
          passivateAfterEvaluation()
        // $COVERAGE-ON$
      }
    case Snapshot(mid) if mid == id =>
      log.debug("Taking snapshot on actor '{}'", persistenceId)
      saveSnapshot(state)
      context.become(snapshotting(sender()))

    // $COVERAGE-OFF$
    case msg: Msg if msg.id != id =>
      log.warning("Unexpected message id '{}' received in actor with id '{}'", msg.id, persistenceId)
      sender() ! UnexpectedMsgId(id, msg.id)
      passivateAfterEvaluation()
    // $COVERAGE-ON$
  }

  private def evaluating(cmd: Command, previous: ActorRef): Receive = {
    case GetLastSeqNr(mid) if mid == id =>
      sender() ! LastSeqNr(id, lastSequenceNr)
      log.debug("Replied with LastSeqNr '{}' from actor '{}'", lastSequenceNr, persistenceId)
      passivateAfterEvaluation()
    case GetCurrentState(mid) if mid == id =>
      sender() ! CurrentState(id, state)
      log.debug("Replied with CurrentState '{}' from actor '{}'", state, persistenceId)
      passivateAfterEvaluation()
    case Left(Rejection(rejection)) =>
      previous ! Evaluated[Rejection, State](id, Left(rejection))
      log.debug("Rejected command '{}' on actor '{}' because '{}'", cmd, persistenceId, rejection)
      context.become(receiveCommand)
      unstashAll()
      passivateAfterEvaluation(Some(cmd))
    case Right(Event(event)) =>
      persist(event) { _ =>
        state = next(state, event)
        previous ! Evaluated[Rejection, Event](id, Right(event))
        log.debug("Applied event '{}' to actor '{}'", event, persistenceId)
        context.become(receiveCommand)
        unstashAll()
        passivateAfterEvaluation(Some(cmd))
      }
    case cet: CommandEvaluationTimeout[_] =>
      log.debug("Returning the command evaluation timeout on actor '{}' to the sender", persistenceId)
      previous ! cet
      context.become(receiveCommand)
      unstashAll()
      passivateAfterEvaluation(Some(cmd))
    case cee: CommandEvaluationError[_] =>
      log.debug("Returning the command evaluation error on actor '{}' to the sender", persistenceId)
      previous ! cee
      context.become(receiveCommand)
      unstashAll()
      passivateAfterEvaluation(Some(cmd))
    // $COVERAGE-OFF$
    case msg: Msg if msg.id != id =>
      log.warning("Unexpected message id '{}' received in actor with id '{}'", msg.id, persistenceId)
      sender() ! UnexpectedMsgId(id, msg.id)
      passivateAfterEvaluation()
    // $COVERAGE-ON$
    case other =>
      log.debug("New message '{}' received for '{}' while evaluating a command, stashing", other, persistenceId)
      stash()
  }

  private def testing(cmd: Command, previous: ActorRef): Receive = {
    case GetLastSeqNr(mid) if mid == id =>
      sender() ! LastSeqNr(id, lastSequenceNr)
      log.debug("Replied with LastSeqNr '{}' from actor '{}'", lastSequenceNr, persistenceId)
      passivateAfterEvaluation()
    case GetCurrentState(mid) if mid == id =>
      sender() ! CurrentState(id, state)
      log.debug("Replied with CurrentState '{}' from actor '{}'", state, persistenceId)
      passivateAfterEvaluation()
    case Left(Rejection(rejection)) =>
      previous ! Tested[Rejection, State](id, Left(rejection))
      log.debug("Rejected test command '{}' on actor '{}' because '{}'", cmd, persistenceId, rejection)
      context.become(receiveCommand)
      unstashAll()
      passivateAfterEvaluation()
    case Right(Event(event)) =>
      previous ! Tested[Rejection, Event](id, Right(event))
      log.debug("Accepted test command '{}' on actor '{}' producing '{}'", cmd, persistenceId, event)
      context.become(receiveCommand)
      unstashAll()
      passivateAfterEvaluation()
    case cet: CommandEvaluationTimeout[_] =>
      log.debug("Returning the command testing timeout on actor '{}' to the sender", persistenceId)
      previous ! cet
      context.become(receiveCommand)
      unstashAll()
      passivateAfterEvaluation(Some(cmd))
    case cee: CommandEvaluationError[_] =>
      log.debug("Returning the command testing error on actor '{}' to the sender", persistenceId)
      previous ! cee
      context.become(receiveCommand)
      unstashAll()
      passivateAfterEvaluation(Some(cmd))
    case msg: Msg if msg.id != id =>
      // $COVERAGE-OFF$
      log.warning("Unexpected message id '{}' received in actor with id '{}'", msg.id, persistenceId)
      sender() ! UnexpectedMsgId(id, msg.id)
      passivateAfterEvaluation()
    // $COVERAGE-ON$
    case other =>
      log.debug("New message '{}' received for '{}' while testing a command, stashing", other, persistenceId)
      stash()
  }

  private def snapshotting(previous: ActorRef): Receive = {
    case SaveSnapshotSuccess(metadata) =>
      previous ! Snapshotted(id, metadata.sequenceNr)
      log.debug("Saved snapshot on '{}' seq nr '{}'", persistenceId, metadata.sequenceNr)
      context.become(receiveCommand)
      unstashAll()
    // $COVERAGE-OFF$
    case GetLastSeqNr(mid) if mid == id =>
      sender() ! LastSeqNr(id, lastSequenceNr)
      log.debug("Replied with LastSeqNr '{}' from actor '{}'", lastSequenceNr, persistenceId)
      passivateAfterEvaluation()
    case GetCurrentState(mid) if mid == id =>
      sender() ! CurrentState(id, state)
      log.debug("Replied with CurrentState '{}' from actor '{}'", state, persistenceId)
      passivateAfterEvaluation()
    case SaveSnapshotFailure(metadata, cause) =>
      previous ! SnapshotFailed(id)
      log.error(cause, "Failed to save snapshot on '{}', seq nr '{}'", persistenceId, metadata.sequenceNr)
      context.become(receiveCommand)
      unstashAll()
    case msg: Msg if msg.id != id =>
      log.warning("Unexpected message id '{}' received in actor with id '{}'", msg.id, persistenceId)
      sender() ! UnexpectedMsgId(id, msg.id)
      passivateAfterEvaluation()
    case other =>
      log.debug("New message '{}' received for '{}' while creating a snapshot, stashing", other, persistenceId)
      stash()
    // $COVERAGE-ON$
  }

  // $COVERAGE-OFF$
  override def unhandled(message: Any): Unit = message match {
    case ReceiveTimeout => passivate()
    case Done(_)        => context.stop(self)
    case other =>
      log.error("Received unknown message '{}' for actor with id '{}'", other, persistenceId)
      super.unhandled(other)
      passivateAfterEvaluation()
  }
  // $COVERAGE-ON$

  private def passivateAfterEvaluation(cmd: Option[Command] = None): Unit =
    if (passivationStrategy.afterEvaluation(name, id, state, cmd)) {
      log.debug("Passivating actor with id '{}' as a result of the per evaluation passivation strategy", persistenceId)
      passivate()
    }

  private def evaluateCommand(cmd: Command, test: Boolean = false): Unit = {
    val scope = if (test) "testing" else "evaluating"
    val eval = for {
      _ <- IO.shift(config.commandEvalExecutionContext)
      r <- evaluate(state, cmd).toIO.timeout(config.commandEvaluationMaxDuration)
      _ <- IO.shift(context.dispatcher)
      _ <- IO(self ! r)
    } yield ()
    val io = eval.onError {
      case th: TimeoutException =>
        log.error(th, s"Timed out while $scope command '{}' on actor '{}'", cmd, persistenceId)
        IO.shift(context.dispatcher) *> IO(self ! CommandEvaluationTimeout(id, cmd))
      case NonFatal(th) =>
        log.error(th, s"Error while $scope command '{}' on actor '{}'", cmd, persistenceId)
        IO.shift(context.dispatcher) *> IO(self ! CommandEvaluationError(id, cmd, Option(th.getMessage)))
    }
    io.unsafeRunAsyncAndForget()
  }
}

private[akka] final case class Terminated(id: String, ref: ActorRef)

//noinspection ActorMutableStateInspection
private[akka] class ParentAggregateActor(name: String, childProps: String => Props) extends Actor with ActorLogging {

  private val buffer = mutable.Map.empty[String, Vector[(ActorRef, Msg)]]

  def receive: Receive = {
    case Done(id) if !buffer.contains(id) =>
      val child = sender()
      context.watchWith(child, Terminated(id, child))
      buffer.put(id, Vector.empty)
      child ! Done(id)
    case msg: Msg if buffer.contains(msg.id) =>
      // must buffer messages
      val messages = buffer(msg.id) :+ (sender() -> msg)
      val _        = buffer.put(msg.id, messages)
    case msg: Msg =>
      val childName = s"$name-${msg.id}"
      log.debug("Routing message '{}' to child '{}'", msg, childName)
      child(msg.id).forward(msg)
    case Terminated(id, _) if buffer.contains(id) =>
      val messages = buffer(id)
      if (messages.nonEmpty) {
        val newChild = child(id)
        messages.foreach {
          case (s, msg) => newChild.!(msg)(s)
        }
      }
      val _ = buffer.remove(id)
  }

  private def child(id: String): ActorRef = {
    val childName = s"$name-$id"
    context
      .child(childName)
      .getOrElse(context.actorOf(childProps(id), childName))
  }
}

private[akka] class ChildAggregateActor[
    F[_]: Effect,
    Event: ClassTag,
    State: ClassTag,
    Command: ClassTag,
    Rejection: ClassTag
](
    override val id: String,
    name: String,
    initialState: State,
    next: (State, Event) => State,
    evaluate: (State, Command) => F[Either[Rejection, Event]],
    passivationStrategy: PassivationStrategy[State, Command],
    config: AkkaSourcingConfig,
) extends AggregateActor[
      F,
      Event,
      State,
      Command,
      Rejection
    ](name, initialState, next, evaluate, passivationStrategy, config) {

  //noinspection ActorMutableStateInspection
  private var passivateRequested: Boolean = false

  override def passivate(): Unit = {
    if (!passivateRequested) {
      context.parent ! Done(id)
      log.debug("Scheduled passivation for actor with name '{}' and id '{}'", name, id)
      passivateRequested = true
    }
  }
}

private[akka] class ShardedAggregateActor[
    F[_]: Effect,
    Event: ClassTag,
    State: ClassTag,
    Command: ClassTag,
    Rejection: ClassTag
](
    name: String,
    initialState: State,
    next: (State, Event) => State,
    evaluate: (State, Command) => F[Either[Rejection, Event]],
    passivationStrategy: PassivationStrategy[State, Command],
    config: AkkaSourcingConfig,
) extends AggregateActor[
      F,
      Event,
      State,
      Command,
      Rejection
    ](name, initialState, next, evaluate, passivationStrategy, config) {

  override def id: String = URLDecoder.decode(self.path.name, "UTF-8")

  //noinspection ActorMutableStateInspection
  private var passivateRequested: Boolean = false

  override def passivate(): Unit = {
    if (!passivateRequested) {
      context.parent ! Passivate(stopMessage = Done(id))
      log.debug("Scheduled passivation for actor with name '{}' and id '{}'", name, id)
      passivateRequested = true
    }
  }
}

object AggregateActor {

  @SuppressWarnings(Array("MaxParameters"))
  private[akka] def parentProps[
      F[_]: Effect,
      Event: ClassTag,
      State: ClassTag,
      Command: ClassTag,
      Rejection: ClassTag
  ](
      name: String,
      initialState: State,
      next: (State, Event) => State,
      evaluate: (State, Command) => F[Either[Rejection, Event]],
      passivationStrategy: PassivationStrategy[State, Command],
      config: AkkaSourcingConfig
  ): Props =
    Props(new ParentAggregateActor(name, childProps(name, initialState, next, evaluate, passivationStrategy, config)))

  @SuppressWarnings(Array("MaxParameters"))
  private[akka] def childProps[
      F[_]: Effect,
      Event: ClassTag,
      State: ClassTag,
      Command: ClassTag,
      Rejection: ClassTag
  ](
      name: String,
      initialState: State,
      next: (State, Event) => State,
      evaluate: (State, Command) => F[Either[Rejection, Event]],
      passivationStrategy: PassivationStrategy[State, Command],
      config: AkkaSourcingConfig,
  )(id: String): Props =
    Props(new ChildAggregateActor(id, name, initialState, next, evaluate, passivationStrategy, config))

  @SuppressWarnings(Array("MaxParameters"))
  private[akka] def shardedProps[
      F[_]: Effect,
      Event: ClassTag,
      State: ClassTag,
      Command: ClassTag,
      Rejection: ClassTag
  ](
      name: String,
      initialState: State,
      next: (State, Event) => State,
      evaluate: (State, Command) => F[Either[Rejection, Event]],
      passivationStrategy: PassivationStrategy[State, Command],
      config: AkkaSourcingConfig
  ): Props =
    Props(new ShardedAggregateActor(name, initialState, next, evaluate, passivationStrategy, config))
}
