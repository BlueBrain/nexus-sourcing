package ch.epfl.bluebrain.nexus.sourcing.stream

import _root_.akka.Done
import _root_.akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props, Status}
import _root_.akka.pattern.pipe
import _root_.akka.stream.scaladsl.{Keep, RunnableGraph, Sink, Source}
import _root_.akka.stream.{ActorMaterializer, KillSwitches, UniqueKillSwitch}
import cats.effect.Effect
import cats.implicits._
import ch.epfl.bluebrain.nexus.sourcing.stream.StreamCoordinator._
import monix.execution.Scheduler
import shapeless.Typeable

import scala.concurrent.Future

/**
  * Actor implementation that builds and manages a stream ([[RunnableGraph]]).
  *
  * @param init   an initialization function to be run when the actor starts, or when the stream is restarted
  * @param source an initialization function that produces a stream from an initial start value
  */
class StreamCoordinator[F[_], A: Typeable](init: F[A], source: A => Source[A, _])(implicit sc: Scheduler, F: Effect[F])
    extends Actor
    with ActorLogging {

  private val A                              = implicitly[Typeable[A]]
  private implicit val as: ActorSystem       = context.system
  private implicit val mt: ActorMaterializer = ActorMaterializer()
  private var state: Option[A]               = None

  private def initialize(): Unit = {
    val logError: PartialFunction[Throwable, F[Unit]] = {
      case err =>
        log.error(err, "Failed on initialize function with error '{}'", err.getMessage)
        F.raiseError(err)
    }
    val _ = F.toIO(init.map(Start).onError(logError)).unsafeToFuture() pipeTo self
  }

  override def preStart(): Unit = {
    super.preStart()
    initialize()
  }

  private def buildStream(a: A): RunnableGraph[(UniqueKillSwitch, Future[Done])] = {
    source(a)
      .viaMat(KillSwitches.single)(Keep.right)
      .map { latest =>
        state = Some(latest)
        latest
      }
      .toMat(Sink.ignore)(Keep.both)
  }

  override def receive: Receive = {
    case Start(any) =>
      A.cast(any) match {
        case Some(a) =>
          log.info(
            "Received initial start value of type '{}', with value '{}' running the indexing function across the element stream",
            A.describe,
            a)
          state = Some(a)
          val (killSwitch, doneFuture) = buildStream(a).run()
          doneFuture pipeTo self
          context.become(running(killSwitch))
        // $COVERAGE-OFF$
        case _ =>
          log.error("Received unknown initial start value '{}', expecting type '{}', stopping", any, A.describe)
          context.stop(self)
        // $COVERAGE-ON$
      }
    // $COVERAGE-OFF$
    case Stop =>
      log.info("Received stop signal while waiting for a start value, stopping")
      context.stop(self)
    // $COVERAGE-ON$

    case FetchLatestState => sender() ! LatestState(state)
  }

  private def running(killSwitch: UniqueKillSwitch): Receive = {
    case Done =>
      log.error("Stream finished unexpectedly, restarting")
      killSwitch.shutdown()
      initialize()
      context.become(receive)
    // $COVERAGE-OFF$
    case Status.Failure(th) =>
      log.error(th, "Stream finished unexpectedly with an error")
      killSwitch.shutdown()
      initialize()
      context.become(receive)
    // $COVERAGE-ON$
    case Stop =>
      log.info("Received stop signal, stopping stream")
      killSwitch.shutdown()
      context.become(stopping)
    case FetchLatestState => sender() ! LatestState(state)
  }

  private def stopping: Receive = {
    case Done =>
      log.info("Stream finished, stopping")
      context.stop(self)
    // $COVERAGE-OFF$
    case Status.Failure(th) =>
      log.error("Stream finished with an error", th)
      context.stop(self)
    // $COVERAGE-ON$
    case FetchLatestState => sender() ! LatestState(state)
  }
}

object StreamCoordinator {
  private[sourcing] final case class Start(any: Any)
  final case object Stop
  final case object FetchLatestState
  final case class LatestState[A](state: Option[A])

  /**
    * Builds a [[Props]] for a [[StreamCoordinator]] with its configuration.
    *
    * @param init   an initialization function to be run when the actor starts, or when the stream is restarted
    * @param source an initialization function that produces a stream from an initial start value
    */
  // $COVERAGE-OFF$
  final def props[F[_]: Effect, A: Typeable](init: F[A], source: A => Source[A, _])(implicit sc: Scheduler): Props =
    Props(new StreamCoordinator(init, source))

  /**
    * Builds a singleton actor of type [[StreamCoordinator]].
    *
    * @param init   an initialization function to be run when the actor starts, or when the stream is restarted
    * @param source an initialization function that produces a stream from an initial start value
    */
  final def start[F[_]: Effect, A: Typeable](init: F[A], source: A => Source[A, _], name: String)(
      implicit as: ActorSystem,
      sc: Scheduler): ActorRef =
    as.actorOf(props(init, source), name)
  // $COVERAGE-ON$
}
