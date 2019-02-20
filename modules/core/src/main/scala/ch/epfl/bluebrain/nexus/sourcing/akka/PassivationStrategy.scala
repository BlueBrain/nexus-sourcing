package ch.epfl.bluebrain.nexus.sourcing.akka
import com.github.ghik.silencer.silent

import scala.concurrent.duration.FiniteDuration

/**
  * A passivation strategy for aggregates backed by persistent actors.
  *
  * @tparam State   the state type
  * @tparam Command the command type
  */
trait PassivationStrategy[State, Command] {

  /**
    * @return Some(duration) if the actor should passivate when inactive for the provided duration, None for no
    *         passivation
    */
  def lapsedSinceLastInteraction: Option[FiniteDuration]

  /**
    * @return Some(duration) if the actor should passivate after the provided duration, None for no passivation
    */
  def lapsedSinceRecoveryCompleted: Option[FiniteDuration]

  /**
    * Function that computes whether the actor should passivate after an evaluation based on the provided arguments.
    *
    * @param name        the name of the aggregate
    * @param id          the id of the aggregate
    * @param state       the state of the aggregate
    * @param lastCommand the last evaluated command
    * @return true whether the actor should be passivated, false otherwise
    */
  def afterEvaluation(name: String, id: String, state: State, lastCommand: Option[Command]): Boolean
}

//noinspection ConvertibleToMethodValue
object PassivationStrategy {

  @silent
  @SuppressWarnings(Array("UnusedMethodParameter"))
  private def neverPassivateFn[State, Command](
      name: String,
      id: String,
      state: State,
      lastCommand: Option[Command]
  ): Boolean = false

  def apply[State, Command](
      sinceLast: Option[FiniteDuration],
      sinceRecovered: Option[FiniteDuration],
      shouldPassivate: (String, String, State, Option[Command]) => Boolean
  ): PassivationStrategy[State, Command] =
    new PassivationStrategy[State, Command] {
      override val lapsedSinceLastInteraction: Option[FiniteDuration]   = sinceLast
      override val lapsedSinceRecoveryCompleted: Option[FiniteDuration] = sinceRecovered
      override def afterEvaluation(name: String, id: String, state: State, lastCommand: Option[Command]): Boolean =
        shouldPassivate(name, id, state, lastCommand)
    }

  /**
    * A passivation strategy that never executes.
    *
    * @tparam State   the state type
    * @tparam Command the command type
    */
  def never[State, Command]: PassivationStrategy[State, Command] =
    apply(None, None, neverPassivateFn)

  /**
    * A passivation strategy that executes after each processed message.
    *
    * @tparam State   the state type
    * @tparam Command the command type
    */
  def immediately[State, Command]: PassivationStrategy[State, Command] =
    apply(None, None, (_: String, _: String, _: State, _: Option[Command]) => true)

  /**
    * A passivation strategy that executes based on the interval lapsed since the last interaction with the aggregate
    * actor.
    *
    * @param duration        the interval duration
    * @param shouldPassivate if provided, the function will be evaluated after each message exchanged; its result will
    *                        determine if the aggregate will be passivated
    * @tparam State          the state type
    * @tparam Command        the command type
    * @return
    */
  def lapsedSinceLastInteraction[State, Command](
      duration: FiniteDuration,
      shouldPassivate: (String, String, State, Option[Command]) => Boolean = neverPassivateFn _
  ): PassivationStrategy[State, Command] =
    apply(Some(duration), None, shouldPassivate)

  /**
    * A passivation strategy that executes based on the interval lapsed since the aggregate actor has recovered its
    * state (which happens immediately after the actor is started).
    *
    * @param duration        the interval duration
    * @param shouldPassivate if provided, the function will be evaluated after each message exchanged; its result will
    *                        determine if the aggregate will be passivated
    * @tparam State          the state type
    * @tparam Command        the command type
    * @return
    */
  def lapsedSinceRecoveryCompleted[State, Command](
      duration: FiniteDuration,
      shouldPassivate: (String, String, State, Option[Command]) => Boolean = neverPassivateFn _
  ): PassivationStrategy[State, Command] =
    apply(None, Some(duration), shouldPassivate)
}
