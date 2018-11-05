[![Join the chat at https://gitter.im/BlueBrain/nexus](https://badges.gitter.im/BlueBrain/nexus.svg)](https://gitter.im/BlueBrain/nexus?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![GitHub release](https://img.shields.io/github/release/BlueBrain/nexus-sourcing.svg)]()

# nexus-sourcing

A small eventsourcing scala library that models aggregates as FSMs.

The `akka` modules provides an implementation based on `akka-persistence`, `akka-persistence-query` and
`akka-cluster-sharding`.

### Example usage

Considering the following `Event`, `State`, `Command` and `Rejection` types:
```
sealed trait Event extends Product with Serializable {
  def rev: Int
}
object Event {
  final case class Incremented(rev: Int, step: Int) extends Event
  final case class Initialized(rev: Int)            extends Event
}
```
```
sealed trait State extends Product with Serializable
object State {
  final case object Initial                      extends State
  final case class Current(rev: Int, value: Int) extends State
}
```
```
import scala.concurrent.duration.FiniteDuration

sealed trait Command extends Product with Serializable {
  def rev: Int
}
object Command {
  final case class Increment(rev: Int, step: Int)                             extends Command
  final case class IncrementAsync(rev: Int, step: Int, sleep: FiniteDuration) extends Command
  final case class Initialize(rev: Int)                                       extends Command
  final case class Boom(rev: Int, message: String)                            extends Command
  final case class Never(rev: Int)                                            extends Command
}
```
```
sealed trait Rejection extends Product with Serializable
object Rejection {
  final case class InvalidRevision(rev: Int) extends Rejection
}
```

And considering the following functions that describe how recovery and evaluations are executed (notice that the
`evaluate` is defined for an arbitrary `F[_]` type that has `Timer[F]` and `Async` type class instances from
`cats-effect`):

```
val initialState: State = State.Initial

val next: (State, Event) => State = {
  case (Initial, Incremented(1, step))             => State.Current(1, step)
  case (Initial, Initialized(rev))                 => State.Current(rev, 0)
  case (Current(_, value), Incremented(rev, step)) => State.Current(rev, value + step)
  case (Current(_, _), Initialized(rev))           => State.Current(rev, 0)
  case (other, _)                                  => other
}

def evaluate[F[_]](state: State, cmd: Command)(implicit F: Async[F], T: Timer[F]): F[Either[Rejection, Event]] =
  (state, cmd) match {
    case (Current(revS, _), Boom(revC, message)) if revS == revC => F.raiseError(new RuntimeException(message))
    case (Initial, Boom(rev, message)) if rev == 0               => F.raiseError(new RuntimeException(message))
    case (_, Boom(rev, _))                                       => F.pure(Left(InvalidRevision(rev)))
    case (Current(revS, _), Never(revC)) if revS == revC         => F.never
    case (Initial, Never(rev)) if rev == 0                       => F.never
    case (_, Never(rev))                                         => F.pure(Left(InvalidRevision(rev)))
    case (Initial, Increment(rev, step)) if rev == 0             => F.pure(Right(Incremented(1, step)))
    case (Initial, Increment(rev, _))                            => F.pure(Left(InvalidRevision(rev)))
    case (Initial, IncrementAsync(rev, step, duration)) if rev == 0 =>
      T.sleep(duration) *> F.pure(Right(Incremented(1, step)))
    case (Initial, IncrementAsync(rev, _, _))                      => F.pure(Left(InvalidRevision(rev)))
    case (Initial, Initialize(rev)) if rev == 0                    => F.pure(Right(Initialized(1)))
    case (Initial, Initialize(rev))                                => F.pure(Left(InvalidRevision(rev)))
    case (Current(revS, _), Increment(revC, step)) if revS == revC => F.pure(Right(Incremented(revS + 1, step)))
    case (Current(_, _), Increment(revC, _))                       => F.pure(Left(InvalidRevision(revC)))
    case (Current(revS, _), IncrementAsync(revC, step, duration)) if revS == revC =>
      T.sleep(duration) *> F.pure(Right(Incremented(revS + 1, step)))
    case (Current(_, _), IncrementAsync(revC, _, duration)) =>
      T.sleep(duration) *> F.pure(Left(InvalidRevision(revC)))
    case (Current(revS, _), Initialize(revC)) if revS == revC => F.pure(Right(Initialized(revS + 1)))
    case (Current(_, _), Initialize(rev))                     => F.pure(Left(InvalidRevision(rev)))
  }
```

Here's an example of using an in memory aggregate (the `unsafeRunSync` is invocation is not necessary, it is just
to show how it works):
```
implicit val ctx   = IO.contextShift(ExecutionContext.global)
implicit val timer = IO.timer(ExecutionContext.global)

val agg = Aggregate.inMemory[IO, Int]("global", initialState, next, evaluate[IO]).unsafeRunSync()

agg.name // "global"
agg.evaluate(1, Increment(0, 2)).unsafeRunSync() // Right(Incremented(1, 2))
agg.evaluate(1, IncrementAsync(1, 5, 200 millis)).unsafeRunSync() // Right(Incremented(2, 5))
agg.lastSequenceNr(1).unsafeRunSync() // 2L
agg.test(1, Initialize(0)).unsafeRunSync() // Left(InvalidRevision(0))
agg.test(1, Initialize(2)).unsafeRunSync() // Right(Initialized(3))
agg.currentState(1).unsafeRunSync() // Current(2, 7)
agg.exists(1).unsafeRunSync() // true
agg.snapshot(1).unsafeRunSync() // 2L
agg.evaluate(1, Initialize(2)).unsafeRunSync() // Right(Initialized(3))
agg.foldLeft(1, 0) {
  case (initializedCount, Initialized(_)) => initializedCount + 1
  case (initializedCount, _)              => initializedCount
}.unsafeRunSync() // 1
```

Constructing an akka based aggregate using a deployment of actors in the same actor system:
```
implicit val system: ActorSystem    = ActorSystem()
implicit val mat: ActorMaterializer = ActorMaterializer()
implicit val ctx: ContextShift[IO]  = IO.contextShift(ExecutionContext.global)
implicit val timer: Timer[IO]       = IO.timer(ExecutionContext.global)

val config = AkkaSourcingConfig(
  Timeout(1.second),       // maximum duration to wait for a reply from an aggregate actor
  "inmemory-read-journal", // the persistence query journal (from "com.github.dnvriend" %% "akka-persistence-inmemory")
  200 milliseconds,        // command evaluation timeout
  ExecutionContext.global  // the execution context where commands are evaluated
)

val passivation = PassivationStrategy.immediately[State, Command]
val retry       = RetryStrategy.once[IO]
val name        = "my-entity-type"
val agg = AkkaAggregate
  .tree[IO](name, initialState, next, evaluate[IO], passivation, retry, config, poolSize = 10)
  .unsafeRunSync()
```

Constructing an akka based aggregate using a deployment of actors spread across the cluster:
```
implicit val system: ActorSystem    = ActorSystem()
implicit val mat: ActorMaterializer = ActorMaterializer()
implicit val ctx: ContextShift[IO]  = IO.contextShift(ExecutionContext.global)
implicit val timer: Timer[IO]       = IO.timer(ExecutionContext.global)

val config = AkkaSourcingConfig(
  Timeout(1.second),       // maximum duration to wait for a reply from an aggregate actor
  "inmemory-read-journal", // the persistence query journal (from "com.github.dnvriend" %% "akka-persistence-inmemory")
  200 milliseconds,        // command evaluation timeout
  ExecutionContext.global  // the execution context where commands are evaluated
)

val passivation = PassivationStrategy.immediately[State, Command]
val retry       = RetryStrategy.once[IO]
val name        = "my-entity-type"
val agg = AkkaAggregate
  .sharded[IO](name, initialState, next, evaluate[IO], passivation, retry, config, shards = 10)
  .unsafeRunSync()
```