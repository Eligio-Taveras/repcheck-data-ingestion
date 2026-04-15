package repcheck.ingestion.bills.metadata.errors

final case class BillFetchFailed(
  endpoint: String,
  statusCode: Int,
  detail: String,
  cause: Throwable,
) extends Exception(
      s"Failed to fetch bills from $endpoint (HTTP $statusCode): $detail",
      cause,
    )
