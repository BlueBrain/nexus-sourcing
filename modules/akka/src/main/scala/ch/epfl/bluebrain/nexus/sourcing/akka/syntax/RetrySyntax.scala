package ch.epfl.bluebrain.nexus.sourcing.akka.syntax

import ch.epfl.bluebrain.nexus.sourcing.akka.syntax.RetrySyntax.RetryOps
import ch.epfl.bluebrain.nexus.sourcing.akka.{Retry, RetryMap}

class RetrySyntax {
  implicit def retrySyntax[F[_], A](fa: F[A]) = new RetryOps[F, A](fa)
}

object RetrySyntax {
  private[syntax] class RetryOps[F[_], A](private val fa: F[A]) extends AnyVal {

    /**
      * Returns a new value in the same context type, but with a preconfigured retry mechanism.
      *
      * @return a new value in the same context type, but with a preconfigured retry mechanism
      */
    def retry(implicit retryer: Retry[F]): F[A] =
      retryer.apply(fa)

    /**
      * Returns a new value computed from the ''pf''. Retries with a preconfigured retry mechanism
      * if an error [[E]] occurs or if ''pf'' was not defined.
      *
      * @param pf a partial function to transform A into B
      * @tparam B the type of the output value
      * @return a new value computed from the ''pf'' in the same context type
      */
    def mapRetry[E, B](pf: PartialFunction[A, B], onMapFailure: => E)(implicit retryer: RetryMap[F, E]): F[B] =
      retryer.apply(fa, pf, onMapFailure)

  }
}
