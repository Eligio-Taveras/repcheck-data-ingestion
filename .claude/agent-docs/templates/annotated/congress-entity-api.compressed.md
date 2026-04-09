<!-- GENERATED FILE — DO NOT EDIT. Source: docs/templates/annotated/congress-entity-api.md -->

# Pattern: Congress.gov Entity API Client

## When To Use This Pattern
- Implementing any new Congress.gov entity API client (members, votes, amendments, etc.)
- Adding entity DTOs/DOs to any data-ingestion repo
- Any agent implementing a new ingestion pipeline step

## Related Patterns
- `paginated-api-client.md` — the generic PagingApiBase trait
- `dto-do-layering.md` — explains the 3-layer transformation approach
- `enum-with-parsing.md` — explains the entity type enum pattern
- `congress-entity-api.scala` — skeleton with `???` placeholders
- `gov-apis/src/main/scala/congress/gov/apis/LegislativeBillsApi.scala` — gold-standard reference
- `docs/reference/congress-gov-api.yaml` — full Congress.gov OpenAPI spec

---

## Congress.gov v3 Entity Endpoint

Consult `docs/reference/congress-gov-api.yaml` for endpoint path and response shape.

**URL pattern:**
```
GET https://api.congress.gov/v3/{entity-path}
    ?api_key=YOUR_KEY&offset=0&limit=250&sort=updateDate+desc
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
- `number` — entity identifier within congress (string)
- `type` — `"SAMDT"` (Senate), `"HAMDT"` (House), `"SUAMDT"` (Senate unprinted)
- `proposedDate`, `description`, `purpose` — nullable
- `amendedBill` — lightweight cross-reference, NOT full bill object
- Natural key: `congress` + `type` + `number`

---

## Entity Type Enum

```scala
// File: gov-apis/src/main/scala/congress/gov/DTOs/AmendmentType.scala
package congress.gov.DTOs

case class UnknownAmendmentType(value: String)
    extends Exception(s"Unknown amendment type: '$value'")

enum AmendmentType(val value: String) {
  case SenateAmendment        extends AmendmentType("SAMDT")
  case HouseAmendment         extends AmendmentType("HAMDT")
  case SenatePrintedAmendment extends AmendmentType("SUAMDT")
}

object AmendmentType {
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

## GovSite Decoder

Maps raw Congress.gov JSON field names to internal field names using manual `HCursor` decoder.

```scala
// File: gov-apis/src/main/scala/congress/gov/DTOs/GovSite/AmendmentDTOGovSite.scala
package congress.gov.DTOs.GovSite

import java.time.ZonedDateTime
import io.circe.Decoder
import congress.gov.DTOs.{AmendmentDTO, AmendedBillRef, LatestAction}

object AmendmentDTOGovSite {
  import common.Serializers.*

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
        amendment_id   <- c.downField("number").as[String]
        amendment_type <- c.downField("type").as[String]
        latestAction   <- c.downField("latestAction").as[LatestAction]
        description    <- c.downField("description").as[Option[String]]
        purpose        <- c.downField("purpose").as[Option[String]]
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

## Nested Reference Case Class

Only needed for entities that reference other entities.

```scala
// File: gov-apis/src/main/scala/congress/gov/DTOs/AmendedBillRef.scala
package congress.gov.DTOs

case class AmendedBillRef(
    congress: Int,
    number: String,
    billType: String,
    title: Option[String],
    url: String
)
```

---

## Internal DTO + Container DTO

```scala
// File: gov-apis/src/main/scala/congress/gov/DTOs/AmendmentDTO.scala
package congress.gov.DTOs

import java.time.ZonedDateTime

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
package congress.gov.DTOs

import cats.Semigroup
import cats.effect.Concurrent
import org.http4s.EntityDecoder
import apiBase.PagedObject

case class AmendmentsDTO(amendments: Seq[AmendmentDTO]) extends PagedObject {
  override def lengthRetrieved: Int = amendments.length
}

object AmendmentsDTO {
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

## Internal Circe Codecs

```scala
// File: gov-apis/src/main/scala/congress/gov/DTOs/Internal/AmendmentDTO.scala
package congress.gov.DTOs.Internal

import cats.effect.Concurrent
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder}
import org.http4s.EntityDecoder
import congress.gov.DTOs.AmendmentDTO

object AmendmentDTO {
  implicit val decoder: Decoder[AmendmentDTO] = deriveDecoder[AmendmentDTO]
  implicit val encoder: Encoder[AmendmentDTO] = deriveEncoder[AmendmentDTO]

  implicit def entityDecoder[F[_]: Concurrent]: EntityDecoder[F, AmendmentDTO] =
    org.http4s.circe.jsonOf[F, AmendmentDTO]
}
```

---

## Domain Object

```scala
// File: gov-apis/src/main/scala/congress/gov/DOs/AmendmentDO.scala
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
    amendmentType: AmendmentType,
    latestAction: LatestAction,
    description: Option[String],
    purpose: Option[String],
    proposedDate: Option[ZonedDateTime],
    submittedDate: Option[ZonedDateTime],
    updateDate: ZonedDateTime,
    url: Uri,
    amendedBill: Option[AmendedBillRef]
) {
  def rowId: String = s"${congress}-${amendmentType.value}-${amendmentId}"

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

## API Client

```scala
// File: gov-apis/src/main/scala/congress/gov/apis/AmendmentsApi.scala
package congress.gov.apis

import cats.effect.{Async, Resource}
import cats.syntax.all._
import org.http4s.EntityDecoder
import org.http4s.client.Client
import org.http4s.ember.client.EmberClientBuilder
import fs2.io.net.Network
import apiBase.PagingApiBase
import congress.gov.DTOs.AmendmentsDTO

class AmendmentsApi[F[_]: Async: Network](
    val protocol: String = "https",
    val host: String = "api.congress.gov",
    val apiKey: String = "DEMO_KEY",
    val path: String = "/v3/amendment",
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

## Test Pattern

```scala
// File: gov-apis/src/test/scala/congress/gov/DTOs/AmendmentSpec.scala
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

## Decision Table

| Decision | Rationale |
|---|---|
| Nullable API fields use `Option[T]` | Prevents decode failures on older records omitting the field |
| Nested cross-references are lightweight ref objects | Avoids duplication; related entities fetched independently |
| Natural key is `congress-type-id` | Mirrors Congress.gov URL structure; stable across updates |
| Flat `UnknownXxxType` exception | Project convention: one unique exception per failure case |
| Entity type stays `String` in DTO | Validation deferred to `toDO` — GovSite decoding should not fail on type strings |
| `Tables.xxx` constant for table name | Never hardcode table name strings |
| `toDO` returns `Either[String, DO]` | Pipeline fail-and-continue records the Left as per-item failure without halting |