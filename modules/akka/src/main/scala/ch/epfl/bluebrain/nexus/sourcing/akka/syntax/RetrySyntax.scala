package ch.epfl.bluebrain.nexus.sourcing.akka.syntax

import ch.epfl.bluebrain.nexus.sourcing.akka.Retry
import ch.epfl.bluebrain.nexus.sourcing.akka.syntax.RetrySyntax.RetryOps

class RetrySyntax {
  implicit def retrySyntax[F[_], A](fa: F[A]) = new RetryOps[F, A](fa)
}

object RetrySyntax {

  final class RetryOps[F[_], A](private val fa: F[A]) extends AnyVal {

    /**
      * Returns a new value in the same context type, but with a preconfigured retry mechanism.
      *
      * @return a new value in the same context type, but with a preconfigured retry mechanism
      */
    def retry(implicit retry: Retry[F, _]): F[A] =
      retry.apply(fa)

    /**
      * Returns a new value computed from the ''pf''. Retries with a preconfigured retry mechanism
      * if an error [[E]] occurs or if ''pf'' was not defined.
      *
      * @param pf           a partial function to transform A into B
      * @param onMapFailure an error to fail the computation with when the partial function is not defined
      *                     for the resulting A value
      * @tparam B the type of the output value
      * @return a new value computed from the ''pf'' in the same context type
      */
    def mapRetry[E, B](pf: PartialFunction[A, B], onMapFailure: => E)(implicit retry: Retry[F, E]): F[B] =
      retry.apply(fa, pf, onMapFailure)
  }
}
