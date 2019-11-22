package ch.epfl.bluebrain.nexus.sourcing.projections

import akka.Done
import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, Props, Status}
import akka.cluster.singleton.{ClusterSingletonManager, ClusterSingletonManagerSettings}
import akka.pattern.{ask, pipe}
import akka.stream.scaladsl.{Keep, RunnableGraph, Sink, Source}
import akka.stream.{KillSwitches, UniqueKillSwitch}
import akka.util.Timeout
import cats.effect.syntax.all._
import cats.effect.{ContextShift, Effect, IO}
import cats.implicits._
import ch.epfl.bluebrain.nexus.sourcing.akka.SourcingConfig
import ch.epfl.bluebrain.nexus.sourcing.projections.StreamSupervisor.{FetchLatestState, LatestState, Stop}

import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag

/**
  * Component that supervises a stream through an actor.
  *
  * @param actor the underlying actor
  */
class StreamSupervisor[F[_], A](val actor: ActorRef)(implicit F: Effect[F], as: ActorSystem, config: SourcingConfig) {
  private implicit val ec: ExecutionContext           = as.dispatcher
  private implicit val askTimeout: Timeout            = config.askTimeout
  private implicit val contextShift: ContextShift[IO] = IO.contextShift(ec)

  /**
    * Fetches the latest state from the underlying actor
    * [[ch.epfl.bluebrain.nexus.sourcing.projections.StreamSupervisor.StreamSupervisorActor]] .
    *
    * @return latest state wrapped in [[F]]
    */
  def state()(implicit A: ClassTag[Option[A]]): F[Option[A]] = IO.fromFuture(IO(actor ? FetchLatestState)).to[F].map {
    case LatestState(A(state)) => state
  }

  /**
    * Stops the stream.
    */
  def stop(): F[Unit] = F.pure(actor ! Stop)

}

object StreamSupervisor {
  private[sourcing] final case class Start(any: Any)
  private[sourcing] final case object Stop
  private[sourcing] final case object FetchLatestState
  private[sourcing] final case class LatestState[A](state: Option[A])

  /**
    * Actor implementation that builds and manages a stream ([[RunnableGraph]]).
    *
    * @param init   an initialization function to be run when the actor starts, or when the stream is restarted
    * @param source an initialization function that produces a stream from an initial start value
    */
  class StreamSupervisorActor[F[_], A: ClassTag](init: F[A], source: A => Source[A, _])(implicit F: Effect[F])
      extends Actor
      with ActorLogging {

    private val A                             = implicitly[ClassTag[A]]
    private implicit val as: ActorSystem      = context.system
    private implicit val ec: ExecutionContext = as.dispatcher
    //noinspection ActorMutableStateInspection
    private var state: Option[A] = None

    private def initialize(): Unit = {
      val logError: PartialFunction[Throwable, F[Unit]] = {
        case err =>
          log.error(err, "Failed on initialize function with error '{}'", err.getMessage)
          F.raiseError(err)
      }
      val _ = init.map(Start).onError(logError).toIO.unsafeToFuture() pipeTo self
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
        any match {
          case A(a) =>
            log.info(
              "Received initial start value of type '{}', with value '{}' running the indexing function across the element stream",
              A.runtimeClass.getSimpleName,
              a
            )
            state = Some(a)
            val (killSwitch, doneFuture) = buildStream(a).run()
            doneFuture pipeTo self
            context.become(running(killSwitch))
          // $COVERAGE-OFF$
          case _ =>
            log.error(
              "Received unknown initial start value '{}', expecting type '{}', stopping",
              any,
              A.runtimeClass.getSimpleName
            )
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

  /**
    * Builds a [[Props]] for a [[StreamSupervisorActor]] with its configuration.
    *
    * @param init   an initialization function to be run when the actor starts, or when the stream is restarted
    * @param source an initialization function that produces a stream from an initial start value
    */
  // $COVERAGE-OFF$
  final def props[F[_]: Effect, A: ClassTag](init: F[A], source: A => Source[A, _]): Props =
    Props(new StreamSupervisorActor(init, source))

  /**
    * Builds a [[StreamSupervisor]].
    *
    * @param init    an initialization function to be run when the actor starts, or when the stream is restarted
    * @param source  an initialization function that produces a stream from an initial start value
    * @param name    the actor name
    * @param actorOf a function that given an actor Props and a name it instantiates an ActorRef
    */
  final def start[F[_]: Effect, A: ClassTag](
      init: F[A],
      source: A => Source[A, _],
      name: String,
      actorOf: (Props, String) => ActorRef
  )(
      implicit as: ActorSystem,
      config: SourcingConfig
  ): StreamSupervisor[F, A] =
    new StreamSupervisor[F, A](actorOf(props(init, source), name))

  /**
    * Builds a [[StreamSupervisor]].
    *
    * @param init    an initialization function to be run when the actor starts, or when the stream is restarted
    * @param source  an initialization function that produces a stream from an initial start value
    * @param name    the actor name
    * @param actorOf a function that given an actor Props and a name it instantiates an ActorRef
    */
  final def delay[F[_]: Effect, A: ClassTag](
      init: F[A],
      source: A => Source[A, _],
      name: String,
      actorOf: (Props, String) => ActorRef
  )(
      implicit as: ActorSystem,
      config: SourcingConfig
  ): F[StreamSupervisor[F, A]] =
    Effect[F].delay(start(init, source, name, actorOf))

  /**
    * Builds a [[Props]] for a [[StreamSupervisorActor]] with it cluster singleton configuration.
    *
    * @param init   an initialization function to be run when the actor starts, or when the stream is restarted
    * @param source an initialization function that produces a stream from an initial start value
    */
  final def singletonProps[F[_]: Effect, A: ClassTag](init: F[A], source: A => Source[A, _])(
      implicit as: ActorSystem
  ): Props =
    ClusterSingletonManager.props(
      Props(new StreamSupervisorActor(init, source)),
      terminationMessage = Stop,
      settings = ClusterSingletonManagerSettings(as)
    )

  /**
    * Builds a  [[StreamSupervisor]] based on a cluster singleton actor.
    *
    * @param init    an initialization function to be run when the actor starts, or when the stream is restarted
    * @param source  an initialization function that produces a stream from an initial start value
    * @param name    the actor name
    * @param actorOf a function that given an actor Props and a name it instantiates an ActorRef
    */
  final def startSingleton[F[_]: Effect, A: ClassTag](
      init: F[A],
      source: A => Source[A, _],
      name: String,
      actorOf: (Props, String) => ActorRef
  )(
      implicit as: ActorSystem,
      config: SourcingConfig
  ): StreamSupervisor[F, A] =
    new StreamSupervisor[F, A](actorOf(singletonProps(init, source), name))

  /**
    * Builds a  [[StreamSupervisor]] based on a cluster singleton actor.
    *
    * @param init    an initialization function to be run when the actor starts, or when the stream is restarted
    * @param source  an initialization function that produces a stream from an initial start value
    * @param name    the actor name
    * @param actorOf a function that given an actor Props and a name it instantiates an ActorRef
    */
  final def delaySingleton[F[_]: Effect, A: ClassTag](
      init: F[A],
      source: A => Source[A, _],
      name: String,
      actorOf: (Props, String) => ActorRef
  )(
      implicit as: ActorSystem,
      config: SourcingConfig
  ): F[StreamSupervisor[F, A]] =
    Effect[F].delay(startSingleton(init, source, name, actorOf))
  // $COVERAGE-ON$
}
