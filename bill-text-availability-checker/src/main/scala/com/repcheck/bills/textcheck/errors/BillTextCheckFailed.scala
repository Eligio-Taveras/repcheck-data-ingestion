package com.repcheck.bills.textcheck.errors

final case class BillTextCheckFailed(
  billId: String,
  detail: String,
  cause: Throwable,
) extends Exception(
      s"Failed to check text availability for bill $billId: $detail",
      cause,
    )
