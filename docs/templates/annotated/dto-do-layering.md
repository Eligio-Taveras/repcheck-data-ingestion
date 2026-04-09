# Pattern: DTO/DO Layering

## Pattern Summary
A three-layer data transformation pipeline that separates external API shapes from internal logic from storage shapes. External DTOs (GovSite) are decoded from raw JSON, converted to Internal DTOs (our canonical API shape), then transformed into Domain Objects (DOs) for persistence. Each layer has a single responsibility and a clear conversion boundary.

## When To Use This Pattern
- Any new data source (Congress.gov votes, members, amendments)
- Any external API where the response shape differs from your storage shape
- When multiple external sources produce the same logical entity (e.g., bill data from different API versions)

## Source Files
- `gov-apis/src/main/scala/congress/gov/DTOs/GovSite/LegislativeBillDTOGovSite.scala` — GovSite decoder (external shape)
- `gov-apis/src/main/scala/congress/gov/DTOs/LegislativeBillDTO.scala` — Internal DTO (canonical shape)
- `gov-apis/src/main/scala/congress/gov/DTOs/Internal/LegislativeBillDTO.scala` — Circe codecs for Internal DTO
- `gov-apis/src/main/scala/congress/gov/DOs/LegislativeBillDO.scala` — Domain Object (storage shape)

---

## Layer 1: GovSite DTO (External API Shape)

This layer exists because the Congress.gov API field names don't match our internal names. The decoder maps API fields to our canonical DTO.

```scala
// File: gov-apis/src/main/scala/congress/gov/DTOs/GovSite/LegislativeBillDTOGovSite.scala

package congress.gov.DTOs.GovSite

import java.time.{LocalDate, ZonedDateTime}

import io.circe.Decoder

import congress.gov.DTOs.{LatestAction, LegislativeBillDTO}

object LegislativeBillDTOGovSite {

  // ANNOTATION: Import custom ZonedDateTime decoder from our Serializers.
  // This is needed because the API returns dates in a non-standard format.
  import common.Serializers.*

  // ANNOTATION: Manual decoder that maps EXTERNAL field names to INTERNAL field names.
  // The API returns "number" but we call it "bill_id".
  // The API returns "type" but we call it "bill_type".
  // This is the ONLY place where external field names appear in our codebase.
  // Everything downstream uses our canonical names.
  implicit val govSiteDecoder: Decoder[LegislativeBillDTO] =
    (c: io.circe.HCursor) =>
      for {
        congress <- c.downField("congress").as[Int]
        bill_id <- c.downField("number").as[String]     // API: "number" → Internal: "bill_id"
        bill_type <- c.downField("type").as[String]      // API: "type" → Internal: "bill_type"
        latestAction <- c.downField("latestAction").as[LatestAction]
        originChamber <- c.downField("originChamber").as[String]
        originChamberCode <- c.downField("originChamberCode").as[String]
        title <- c.downField("title").as[String]
        updateDate <- c.downField("updateDate").as[LocalDate]
        updateDateIncludingText <- c
          .downField("updateDateIncludingText")
          .as[ZonedDateTime]
        url <- c.downField("url").as[String]
      } yield LegislativeBillDTO(                        // Produces our INTERNAL DTO
        congress,
        bill_id,
        bill_type,
        latestAction,
        originChamber,
        originChamberCode,
        title,
        updateDate,
        updateDateIncludingText,
        url
      )
}
```

## Layer 2: Internal DTO (Canonical Shape)

The internal DTO is what our code works with. It has our naming conventions and includes conversion methods to the Domain Object.

```scala
// File: gov-apis/src/main/scala/congress/gov/DTOs/LegislativeBillDTO.scala

package congress.gov.DTOs

import java.time.{LocalDate, LocalDateTime, LocalTime, ZoneId, ZonedDateTime}

import org.http4s.Uri

import congress.gov.DOs.LegislativeBillDO

case class LegislativeBillDTO(
    congress: Int,
    bill_id: String,
    bill_type: String,              // Still a raw String at the DTO level
    latestAction: LatestAction,
    originChamber: String,
    originChamberCode: String,
    title: String,
    updateDate: LocalDate,          // API gives LocalDate (no time component)
    updateDateIncludingText: ZonedDateTime,
    url: String                     // Still a raw String at the DTO level
) {
  // ANNOTATION: Convenience method — parse the URL string into a typed Uri.
  def uri: Uri = Uri.unsafeFromString(url)

  // ANNOTATION: THE KEY CONVERSION — DTO to Domain Object.
  // Returns Either[String, DO] because the conversion can fail:
  //   - BillTypes.fromString might not recognize the bill_type
  //   - If it fails, we get a Left with an error message
  //   - If it succeeds, we get a Right with the validated DO
  //
  // This is the BOUNDARY between unvalidated API data and validated domain data.
  // After this point, bill_type is a BillTypes enum (not a raw String),
  // updateDate is a ZonedDateTime (not a LocalDate), and url is a Uri.
  def toDO: Either[String, LegislativeBillDO] =
    BillTypes.fromString(bill_type).map { billType =>
      LegislativeBillDO(
        congress,
        bill_id,
        billType,                   // Now a validated BillTypes enum
        latestAction,
        originChamber,
        originChamberCode,
        title,
        // ANNOTATION: LocalDate → ZonedDateTime conversion.
        // The API gives us a date without time; normalize to midnight UTC for storage.
        ZonedDateTime.of(LocalDateTime.of(updateDate, LocalTime.of(0, 0)), ZoneId.of("UTC")),
        updateDateIncludingText,
        Uri.unsafeFromString(url)   // Now a typed Uri (not a raw String)
      )
    }
}
```

## Layer 2b: Internal Codecs (Semi-Auto Derivation)

Separate file for Circe codecs — keeps the DTO case class clean.

```scala
// File: gov-apis/src/main/scala/congress/gov/DTOs/Internal/LegislativeBillDTO.scala

package congress.gov.DTOs.Internal

import cats.effect.Concurrent

import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder}

import org.http4s.EntityDecoder

import congress.gov.DTOs.LegislativeBillDTO

// ANNOTATION: This object provides INTERNAL codecs using semi-auto derivation.
// These are used for internal serialization (e.g., our own JSON in tests, caching).
// The GovSite decoder (Layer 1) is used for EXTERNAL deserialization.
//
// Why separate? Because the GovSite API has different field names ("number" vs "bill_id").
// Semi-auto derivation assumes field names match exactly.
object LegislativeBillDTO {
  implicit val decoder: Decoder[LegislativeBillDTO] =
    deriveDecoder[LegislativeBillDTO]
  implicit val encoder: Encoder[LegislativeBillDTO] =
    deriveEncoder[LegislativeBillDTO]

  // ANNOTATION: http4s EntityDecoder bridges Circe and http4s.
  // This lets you do: client.expect[LegislativeBillDTO](uri)
  // and it will automatically parse the HTTP response body as JSON.
  implicit def entityDecoder[F[_]: Concurrent]
  : EntityDecoder[F, LegislativeBillDTO] =
    org.http4s.circe.jsonOf[F, LegislativeBillDTO]
}
```

## Layer 3: Domain Object (Storage Shape)

The DO is what gets persisted. It has validated types and a Doobie `saveBill` method for AlloyDB.

```scala
// File: gov-apis/src/main/scala/congress/gov/DOs/LegislativeBillDO.scala

package congress.gov.DOs

import java.time.ZonedDateTime
import cats.effect.Async
import doobie._
import doobie.implicits._
import doobie.implicits.javasql._
import org.http4s.Uri
import congress.gov.DTOs.{BillTypes, LatestAction}
import pipeline.models.Tables
import org.slf4j.Logger

case class LegislativeBillDO(
    congress: Int,
    billId: String,                 // Renamed from bill_id (snake_case → camelCase)
    billType: BillTypes,            // Validated enum (not raw String)
    latestAction: LatestAction,
    originChamber: String,
    originChamberCode: String,
    title: String,
    updateDate: ZonedDateTime,      // Full ZonedDateTime (not LocalDate)
    updateDateIncludingText: ZonedDateTime,
    url: Uri                        // Typed Uri (not raw String)
) {
  // ANNOTATION: AlloyDB persistence via Doobie.
  // No manual Java Map needed — Doobie auto-derives Read/Write from case class fields.
  // ON CONFLICT DO UPDATE provides idempotent upsert semantics (same as before).
  // Uses Tables constant — never hardcode table name strings.
  def saveBill[F[_]: Async](xa: Transactor[F], logger: Logger): F[Unit] = {
    val table = Tables.bills
    Async[F].blocking(logger.info(s"Saving legislative bill with URL: ${this.url}")) >>
      sql"""
        INSERT INTO #${table}
          (bill_id, congress, bill_type, latest_action_date, latest_action_text,
           origin_chamber, origin_chamber_code, title,
           update_date, update_date_including_text, url)
        VALUES
          ($billId, $congress, ${billType.toString},
           ${latestAction.actionDate}, ${latestAction.text},
           $originChamber, $originChamberCode, $title,
           $updateDate, $updateDateIncludingText, ${url.toString})
        ON CONFLICT (bill_id) DO UPDATE SET
          latest_action_date        = EXCLUDED.latest_action_date,
          latest_action_text        = EXCLUDED.latest_action_text,
          update_date               = EXCLUDED.update_date,
          update_date_including_text = EXCLUDED.update_date_including_text,
          url                       = EXCLUDED.url
      """.update.run.transact(xa).void
  }
}
```

## The Three-Layer Flow

```
Congress.gov API Response (JSON)
    │
    ▼
GovSite Decoder (Layer 1)
    Maps external field names → internal field names
    "number" → bill_id, "type" → bill_type
    │
    ▼
Internal DTO (Layer 2)
    Raw types: bill_type is String, url is String
    Has toDO method that validates and converts
    │
    ▼
Domain Object (Layer 3)
    Validated types: billType is BillTypes enum, url is Uri
    Has saveBill for AlloyDB persistence via Doobie
```

## How to Create a New DTO/DO Layer

1. **GovSite Decoder** — Create `DTOs/GovSite/YourEntityDTOGovSite.scala` with a manual `Decoder` that maps external field names
2. **Internal DTO** — Create `DTOs/YourEntityDTO.scala` with your canonical field names and a `toDO` method returning `Either[String, YourEntityDO]`
3. **Internal Codecs** — Create `DTOs/Internal/YourEntityDTO.scala` with `deriveDecoder`/`deriveEncoder` for internal use
4. **Domain Object** — Create `DOs/YourEntityDO.scala` with validated types and Doobie persistence methods

Example for a new VoteDO:
```scala
// DTO has raw strings
case class VoteDTO(vote_id: String, chamber: String, result: String, ...) {
  def toDO: Either[String, VoteDO] =
    for {
      chamber <- Chamber.fromString(chamber)
      result <- VoteResult.fromString(result)
    } yield VoteDO(vote_id, chamber, result, ...)
}

// DO has validated enums — persisted via Doobie SQL, no toPojo needed
case class VoteDO(voteId: String, chamber: Chamber, result: VoteResult, ...) {
  def saveVote[F[_]: Async](xa: Transactor[F], logger: Logger): F[Unit] = {
    sql"""INSERT INTO ${Tables.votes} (vote_id, chamber, result, ...)
          VALUES ($voteId, ${chamber.toString}, ${result.toString}, ...)
          ON CONFLICT (vote_id) DO UPDATE SET result = EXCLUDED.result, ...
       """.update.run.transact(xa).void
  }
}
```
