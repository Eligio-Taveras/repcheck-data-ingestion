package com.repcheck.bills.common.persistence

import doobie.{Get, Put}

object DoobieInstances {

  private val parseFloatVector: String => Array[Float] = { pgVector =>
    val trimmed = pgVector.stripPrefix("[").stripSuffix("]")
    if (trimmed.isEmpty) {
      Array.empty[Float]
    } else {
      trimmed.split(",").map(_.trim.toFloat)
    }
  }

  private val formatFloatVector: Array[Float] => String =
    arr => arr.mkString("[", ",", "]")

  implicit val floatArrayGet: Get[Array[Float]] =
    Get[String].map(parseFloatVector)

  implicit val floatArrayPut: Put[Array[Float]] =
    Put[String].contramap(formatFloatVector)

}
