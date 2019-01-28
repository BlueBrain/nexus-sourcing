package ch.epfl.bluebrain.nexus.sourcing.akka

import cats.arrow.FunctionK

/**
  * A strategy to be applied for retrying computations of values in a <strong>lazy</strong> __F__ effect type.
  *
  * @tparam F the type of the effect
  */
trait RetryStrategy[F[_]] {

  /**
    * Returns a new value in the same context type, but with a preconfigured retry mechanism.
    *
    * @param fa the computation value in __F__
    * @tparam A the type of the value
    * @return a new value in the same context type, but with a preconfigured retry mechanism
    */
  def apply[A](fa: F[A]): F[A]

}

object RetryStrategy {

  /**
    * Lifts a polymorphic function into a RetryStrategy.
    *
    * @param f the polymorphic function to lift into a RetryStrategy.
    * @see [[cats.arrow.FunctionK.lift]]
    */
  def apply[F[_]](f: FunctionK[F, F]): RetryStrategy[F] = new RetryStrategy[F] {
    override def apply[A](fa: F[A]): F[A] = f(fa)
  }
}
