package com.repcheck.bills.common.persistence

import cats.effect.kernel.MonadCancelThrow

import doobie.ConnectionIO
import doobie.implicits._
import doobie.util.transactor.Transactor

/**
 * Executes a composed [[ConnectionIO]] program within a single database transaction. Repos return raw [[ConnectionIO]]
 * values; callers compose them in a for-comprehension and hand the result to [[TransactionRunner.run]] for atomic
 * execution.
 */
object TransactionRunner {

  def run[F[_]: MonadCancelThrow, A](xa: Transactor[F])(program: ConnectionIO[A]): F[A] =
    program.transact(xa)

}
