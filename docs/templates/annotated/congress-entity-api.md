# Pattern: Congress.gov Entity API Client

## Pattern Summary
Full implementation blueprint for any Congress.gov v3 paginated entity endpoint (amendments, members, votes, etc.). Follows the identical layering pattern as `LegislativeBillsApi`: GovSite decoder → Internal DTO → Domain Object → container DTO → API client. The existing `PagingApiBase` trait handles all pagination logic — no changes needed there.

All code examples use **amendments** as the worked example. Replace `Amendment`/`SAMDT` etc. with your entity's names and types. Consult `docs/reference/congress-gov-api.yaml` for the exact field names and response shape of your target endpoint.

## When To Use This Pattern
- Implementing any new Congress.gov entity API client (members, votes, amendments, etc.)
- Adding entity DTOs/DOs to any data-ingestion repo
- Any agent implementing a new ingestion pipeline step

## Related Patterns
- `paginated-api-client.md` — the generic PagingApiBase trait (read this first)
- `dto-do-layering.md` — explains the 3-layer transformation approach
- `enum-with-parsing.md` — explains the entity type enum pattern
- `congress-entity-api.scala` — skeleton with `???` placeholders to fill in
- `gov-apis/src/main/scala/congress/gov/apis/LegislativeBillsApi.scala` — gold-standard reference
- `docs/reference/congress-gov-api.yaml` — full Congress.gov OpenAPI spec (local, no internet needed)

---

## Section 1 — Congress.gov v3 Entity Endpoint

Look up your entity's endpoint path and response shape in `docs/reference/congress-gov-api.yaml`.

All list endpoints share the same query parameter pattern:

**URL pattern:**
```
GET https://api.congress.gov/v3/{entity-path}
    ?api_key=YOUR_KEY
    &offset=0
    &limit=250
    &fromDateTime=2024-01-01T00:00:00Z
    &toDateTime=2024-12-31T23:59:59Z
    &sort=updateDate+desc
```

**Worked example — amendments (`/v3/amendment`):**
```json
{
  "amendments": [
    {
      "congress": 118,
      "number": "5",
      "type": "SAMDT",
      "latestAction": {
        "actionDate": "2024-01-15",
        "text": "Amendment SA 5 proposed by Senator Smith."
      },
      "description": "An amendment to strike section 3.",
      "purpose": "To strike section 3 of the bill.",
      "proposedDate": "2024-01-15T00:00:00Z",
      "submittedDate": "2024-01-14T00:00:00Z",
      "updateDate": "2024-01-15T00:00:00Z",
      "url": "https://api.congress.gov/v3/amendment/118/samdt/5",
      "amendedBill": {
        "congress": 118,
        "number": "100",
        "type": "S",
        "title": "A bill to improve infrastructure.",
        "url": "https://api.congress.gov/v3/bill/118/s/100"
      }
    }
  ],
  "pagination": {
    "count": 12345,
    "next": "https://api.congress.gov/v3/amendment?offset=250&limit=250&..."
  }
}
```

**Key field notes (amendments):**
- `number` — entity identifier within the congress (string, not int)
- `type` — `"SAMDT"` (Senate), `"HAMDT"` (House), or `"SUAMDT"` (Senate unprinted)
- `proposedDate` — **nullable**: not all amendments have a proposed date
- `description` / `purpose` — **nullable**: may be absent for older records
- `amendedBill` — lightweight cross-reference, NOT the full bill object
- Natural key: `congress` + `type` + `number` (mirrors Congress.gov URL structure)

**Pagination behavior:** identical across all entities — `offset`/`limit`, stop when `lengthRetrieved < pageSize`.

---

## Section 2 — Entity Type Enum

Follows `BillTypes.scala` exactly: enum with string value, `fromString` returning `Either` with a flat unique exception. Only needed if the entity has a type discriminator field.

```scala
// File: gov-apis/src/main/scala/congress/gov/DTOs/AmendmentType.scala
// RENAME: Replace AmendmentType with your entity's type name (e.g. MemberChamber, VoteResult)
package congress.gov.DTOs

// ANNOTATION: Unique exception per failure case — project convention.
// Name describes the failure precisely; context implied by the executing app.
case class UnknownAmendmentType(value: String)
    extends Exception(s"Unknown amendment type: '$value'")

enum AmendmentType(val value: String) {
  case SenateAmendment        extends AmendmentType("SAMDT")
  case HouseAmendment         extends AmendmentType("HAMDT")
  case SenatePrintedAmendment extends AmendmentType("SUAMDT")
}

object AmendmentType {
  // ANNOTATION: Returns Either, never throws. The Left type is a concrete
  // exception (not String) so callers get a typed error they can inspect.
  // toDO calls this and propagates the Left — the pipeline's fail-and-continue
  // logic catches and records it as a ProcessingResult failure.
  def fromString(value: String): Either[UnknownAmendmentType, AmendmentType] = {
    value match {
      case "SAMDT"  => Right(AmendmentType.SenateAmendment)
      case "HAMDT"  => Right(AmendmentType.HouseAmendment)
      case "SUAMDT" => Right(AmendmentType.SenatePrintedAmendment)
      case other    => Left(UnknownAmendmentType(other))
    }
  }
}
```

---

## Section 3 — GovSite Decoder

Maps the raw Congress.gov JSON field names to internal field names. Manual `HCursor` decoder — same approach as `LegislativeBillDTOGovSite.scala`. Use `docs/reference/congress-gov-api.yaml` to find the exact API field names for your entity.

```scala
// File: gov-apis/src/main/scala/congress/gov/DTOs/GovSite/AmendmentDTOGovSite.scala
// RENAME: Replace AmendmentDTOGovSite / AmendmentDTO / AmendedBillRef with your entity's names
package congress.gov.DTOs.GovSite

import java.time.ZonedDateTime

import io.circe.Decoder

import congress.gov.DTOs.{AmendmentDTO, AmendedBillRef, LatestAction}

object AmendmentDTOGovSite {

  import common.Serializers.*

  // ANNOTATION: Nested reference objects (like amendedBill) get their own decoder.
  // They store only enough fields to cross-reference the related entity.
  // The full related entity is fetched independently by its own API client.
  implicit val amendedBillRefDecoder: Decoder[AmendedBillRef] =
    (c: io.circe.HCursor) =>
      for {
        congress <- c.downField("congress").as[Int]
        number   <- c.downField("number").as[String]
        billType <- c.downField("type").as[String]
        title    <- c.downField("title").as[Option[String]]
        url      <- c.downField("url").as[String]
      } yield AmendedBillRef(congress, number, billType, title, url)

  implicit val govSiteDecoder: Decoder[AmendmentDTO] =
    (c: io.circe.HCursor) =>
      for {
        congress       <- c.downField("congress").as[Int]
        // ANNOTATION: API field "number" → internal field "amendment_id"
        // Rename API fields to descriptive internal names where they differ.
        amendment_id   <- c.downField("number").as[String]
        amendment_type <- c.downField("type").as[String]
        latestAction   <- c.downField("latestAction").as[LatestAction]
        description    <- c.downField("description").as[Option[String]]
        purpose        <- c.downField("purpose").as[Option[String]]
        // ANNOTATION: Use Option[T] for any field the API documents as nullable.
        // This prevents decode failures on records that omit the field.
        proposedDate   <- c.downField("proposedDate").as[Option[ZonedDateTime]]
        submittedDate  <- c.downField("submittedDate").as[Option[ZonedDateTime]]
        updateDate     <- c.downField("updateDate").as[ZonedDateTime]
        url            <- c.downField("url").as[String]
        amendedBill    <- c.downField("amendedBill").as[Option[AmendedBillRef]]
      } yield AmendmentDTO(
        congress, amendment_id, amendment_type, latestAction,
        description, purpose, proposedDate, submittedDate,
        updateDate, url, amendedBill
      )
}
```

---

## Section 4 — Nested Reference Case Class (if applicable)

Only needed if the entity references another entity (e.g. an amendment references a bill).

```scala
// File: gov-apis/src/main/scala/congress/gov/DTOs/AmendedBillRef.scala
// RENAME or OMIT: Only needed for entities that reference other entities.
package congress.gov.DTOs

// ANNOTATION: Lightweight cross-reference. Stored as-is in the DO.
// Cross-referencing to the full entity happens at query time, not ingestion time.
case class AmendedBillRef(
    congress: Int,
    number: String,
    billType: String,
    title: Option[String],
    url: String
)
```

---

## Section 5 — Internal DTO + Container DTO

```scala
// File: gov-apis/src/main/scala/congress/gov/DTOs/AmendmentDTO.scala
// RENAME: Replace Amendment with your entity name throughout.
package congress.gov.DTOs

import java.time.ZonedDateTime

// ANNOTATION: Internal DTO uses field names matching our storage schema.
// Entity type stays String here — type validation is deferred to toDO.
// GovSite decoding should not fail on unknown type strings.
case class AmendmentDTO(
    congress: Int,
    amendment_id: String,
    amendment_type: String,
    latestAction: LatestAction,
    description: Option[String],
    purpose: Option[String],
    proposedDate: Option[ZonedDateTime],
    submittedDate: Option[ZonedDateTime],
    updateDate: ZonedDateTime,
    url: String,
    amendedBill: Option[AmendedBillRef]
) {
  // ANNOTATION: toDO validates the type string into a typed enum.
  // Returns Either so the pipeline records failures per-item without halting.
  def toDO: Either[String, AmendmentDO] = {
    AmendmentType.fromString(amendment_type).map { aType =>
      AmendmentDO(
        congress = congress, amendmentId = amendment_id, amendmentType = aType,
        latestAction = latestAction, description = description, purpose = purpose,
        proposedDate = proposedDate, submittedDate = submittedDate,
        updateDate = updateDate, url = org.http4s.Uri.unsafeFromString(url),
        amendedBill = amendedBill
      )
    }.left.map(_.getMessage)
  }
}

// File: gov-apis/src/main/scala/congress/gov/DTOs/AmendmentsDTO.scala
// RENAME: Replace Amendments/Amendment with your entity's plural/singular names.
package congress.gov.DTOs

import cats.Semigroup
import cats.effect.Concurrent
import org.http4s.EntityDecoder
import apiBase.PagedObject

// ANNOTATION: Container DTO for one page of results.
// Must extend PagedObject (tells PagingApiBase when to stop).
// Must have Semigroup (combines pages with |+|).
case class AmendmentsDTO(amendments: Seq[AmendmentDTO]) extends PagedObject {
  override def lengthRetrieved: Int = amendments.length
}

object AmendmentsDTO {
  // ANNOTATION: Combines two pages: AmendmentsDTO(page1.amendments ++ page2.amendments)
  implicit val semigroup: Semigroup[AmendmentsDTO] =
    (x: AmendmentsDTO, y: AmendmentsDTO) => AmendmentsDTO(x.amendments ++ y.amendments)

  implicit val decoder: io.circe.Decoder[AmendmentsDTO] = {
    import io.circe.generic.semiauto.deriveDecoder
    import congress.gov.DTOs.Internal.AmendmentDTO._
    deriveDecoder[AmendmentsDTO]
  }

  implicit def entityDecoder[F[_]: Concurrent]: EntityDecoder[F, AmendmentsDTO] =
    org.http4s.circe.jsonOf[F, AmendmentsDTO]
}
```

---

## Section 6 — Internal Circe Codecs

```scala
// File: gov-apis/src/main/scala/congress/gov/DTOs/Internal/AmendmentDTO.scala
// RENAME: Replace Amendment with your entity name.
package congress.gov.DTOs.Internal

import cats.effect.Concurrent
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder}
import org.http4s.EntityDecoder
import congress.gov.DTOs.AmendmentDTO

// ANNOTATION: Semi-auto derivation for internal use (pipeline messaging, serialization).
// Separate from the GovSite decoder which handles the API's field names.
object AmendmentDTO {
  implicit val decoder: Decoder[AmendmentDTO] = deriveDecoder[AmendmentDTO]
  implicit val encoder: Encoder[AmendmentDTO] = deriveEncoder[AmendmentDTO]

  implicit def entityDecoder[F[_]: Concurrent]: EntityDecoder[F, AmendmentDTO] =
    org.http4s.circe.jsonOf[F, AmendmentDTO]
}
```

---

## Section 7 — Domain Object

```scala
// File: gov-apis/src/main/scala/congress/gov/DOs/AmendmentDO.scala
// RENAME: Replace Amendment with your entity name throughout.
package congress.gov.DOs

import java.time.ZonedDateTime
import cats.effect.Async
import doobie._
import doobie.implicits._
import doobie.implicits.javasql._
import org.http4s.Uri
import org.slf4j.Logger
import congress.gov.DTOs.{AmendmentType, AmendedBillRef, LatestAction}
import pipeline.models.Tables

case class AmendmentDO(
    congress: Int,
    amendmentId: String,
    amendmentType: AmendmentType,   // Typed enum — validated in toDO
    latestAction: LatestAction,
    description: Option[String],
    purpose: Option[String],
    proposedDate: Option[ZonedDateTime],
    submittedDate: Option[ZonedDateTime],
    updateDate: ZonedDateTime,
    url: Uri,                       // Typed Uri — not String
    amendedBill: Option[AmendedBillRef]
) {
  // ANNOTATION: Natural key mirrors Congress.gov URL structure.
  // Used as the primary key / idempotency key for AlloyDB upserts.
  def rowId: String = s"${congress}-${amendmentType.value}-${amendmentId}"

  // ANNOTATION: Use Tables constant — never hardcode table name strings.
  // ON CONFLICT DO UPDATE provides idempotent upsert semantics.
  def saveAmendment[F[_]: Async](xa: doobie.Transactor[F], logger: Logger): F[Unit] = {
    val table = Tables.amendments
    Async[F].blocking(logger.info(s"Saving amendment: $rowId")) >>
      sql"""
        INSERT INTO #${table}
          (row_id, congress, amendment_id, amendment_type, latest_action_date,
           latest_action_text, description, purpose, proposed_date, submitted_date,
           update_date, url, amended_bill_url)
        VALUES
          ($rowId, $congress, $amendmentId, ${amendmentType.value},
           ${latestAction.actionDate}, ${latestAction.text},
           $description, $purpose, $proposedDate, $submittedDate,
           $updateDate, ${url.toString}, ${amendedBill.map(_.url)})
        ON CONFLICT (row_id) DO UPDATE SET
          latest_action_date = EXCLUDED.latest_action_date,
          latest_action_text = EXCLUDED.latest_action_text,
          description        = EXCLUDED.description,
          purpose            = EXCLUDED.purpose,
          proposed_date      = EXCLUDED.proposed_date,
          submitted_date     = EXCLUDED.submitted_date,
          update_date        = EXCLUDED.update_date,
          url                = EXCLUDED.url,
          amended_bill_url   = EXCLUDED.amended_bill_url
      """.update.run.transact(xa).void
  }
}
```

---

## Section 8 — API Client

```scala
// File: gov-apis/src/main/scala/congress/gov/apis/AmendmentsApi.scala
// RENAME: Replace Amendments/AmendmentsDTO with your entity's names.
// UPDATE: Set path to your entity's Congress.gov endpoint (see congress-gov-api.yaml).
package congress.gov.apis

import cats.effect.{Async, Resource}
import cats.syntax.all._
import org.http4s.EntityDecoder
import org.http4s.client.Client
import org.http4s.ember.client.EmberClientBuilder
import fs2.io.net.Network
import apiBase.PagingApiBase
import congress.gov.DTOs.AmendmentsDTO

// ANNOTATION: Extends PagingApiBase — all pagination logic is inherited.
// This class only provides endpoint-specific configuration.
class AmendmentsApi[F[_]: Async: Network](
    val protocol: String = "https",
    val host: String = "api.congress.gov",
    val apiKey: String = "DEMO_KEY",
    val path: String = "/v3/amendment",   // UPDATE: set to your entity's path
    val pageSize: Int = 250
) extends PagingApiBase[F, AmendmentsDTO] {
  override val emberClient: Resource[F, Client[F]] =
    EmberClientBuilder.default[F].build

  override val decoder: EntityDecoder[F, AmendmentsDTO] =
    AmendmentsDTO.entityDecoder
}

object AmendmentsApi {
  def apply[F[_]: Async: Network](apiKey: String, pageSize: Int): F[AmendmentsApi[F]] =
    new AmendmentsApi[F](apiKey = apiKey, pageSize = pageSize).pure[F]
}
```

---

## Section 9 — Test Pattern

```scala
// File: gov-apis/src/test/scala/congress/gov/DTOs/AmendmentSpec.scala
// RENAME: Replace Amendment with your entity name throughout.
package congress.gov.DTOs

import java.time.{ZoneId, ZonedDateTime}
import io.circe.parser.parse
import io.circe.syntax._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import congress.gov.DTOs.Internal.AmendmentDTO._

class AmendmentSpec extends AnyFlatSpec with Matchers {
  trait Fixture {
    val updateTime: ZonedDateTime =
      ZonedDateTime.of(2024, 1, 15, 0, 0, 0, 0, ZoneId.of("UTC"))

    val amendment: AmendmentDTO = AmendmentDTO(
      congress = 118, amendment_id = "5", amendment_type = "SAMDT",
      latestAction = LatestAction(updateTime, "Amendment SA 5 proposed."),
      description = Some("An amendment to strike section 3."),
      purpose = Some("To strike section 3 of the bill."),
      proposedDate = Some(updateTime), submittedDate = Some(updateTime),
      updateDate = updateTime,
      url = "https://api.congress.gov/v3/amendment/118/samdt/5",
      amendedBill = Some(AmendedBillRef(118, "100", "S", Some("A bill to..."),
                      "https://api.congress.gov/v3/bill/118/s/100"))
    )
  }

  "AmendmentType.fromString" should "parse known types" in {
    AmendmentType.fromString("SAMDT")  shouldBe Right(AmendmentType.SenateAmendment)
    AmendmentType.fromString("HAMDT")  shouldBe Right(AmendmentType.HouseAmendment)
    AmendmentType.fromString("SUAMDT") shouldBe Right(AmendmentType.SenatePrintedAmendment)
  }

  // ANNOTATION: Equivalence class negative test — one unknown covers all unknowns.
  "AmendmentType.fromString" should "return Left for unknown types" in {
    AmendmentType.fromString("UNKNOWN") shouldBe a[Left[_, _]]
    AmendmentType.fromString("UNKNOWN").left.map(_.value) shouldBe Left("UNKNOWN")
  }

  "An AmendmentDTO" should "serialize and deserialize roundtrip" in new Fixture {
    val json = amendment.asJson.noSpaces
    parse(json).flatMap(_.as[AmendmentDTO]) shouldBe Right(amendment)
  }

  "An AmendmentDTO" should "convert to DO when type is valid" in new Fixture {
    amendment.toDO shouldBe a[Right[_, _]]
    amendment.toDO.map(_.amendmentType) shouldBe Right(AmendmentType.SenateAmendment)
  }

  "An AmendmentDTO" should "return Left from toDO when type is invalid" in new Fixture {
    amendment.copy(amendment_type = "BADTYPE").toDO shouldBe a[Left[_, _]]
  }
}
```

---

## Section 10 — Decision Table

| Decision | Rationale |
|---|---|
| Nullable API fields use `Option[T]` | Prevents decode failures on older records that omit the field |
| Nested cross-references are lightweight ref objects | Avoids duplication; related entities fetched independently |
| Natural key is `congress-type-id` | Mirrors Congress.gov URL structure; stable across updates |
| Flat `UnknownXxxType` exception | Project convention: one unique exception per failure case |
| Entity type stays `String` in DTO | Validation deferred to `toDO` — GovSite decoding should not fail on type strings |
| `Tables.xxx` constant for table name | Never hardcode table name strings — use constants from pipeline-models |
| `toDO` returns `Either[String, DO]` | Pipeline fail-and-continue records the Left as a per-item failure without halting |
