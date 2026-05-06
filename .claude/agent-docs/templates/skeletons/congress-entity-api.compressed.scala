<!-- GENERATED FILE — DO NOT EDIT. Source: docs/templates/skeletons/congress-entity-api.scala -->

```markdown
# RepCheck Skeleton: Congress.gov Entity API Client

**PURPOSE:** Generic scaffold for Congress.gov v3 paginated entity endpoints. Replace `ENTITY` placeholders with your entity (members, votes, amendments, etc.).

**STEPS TO IMPLEMENT:**
1. Replace `ENTITY` with entity name
2. Fill in API endpoint path in `ENTITYApi`
3. Define `ENTITYType` enum if entity has type discriminator
4. Define `ENTITYRefDTO` if entity references another entity
5. Add fields to `ENTITYDTO` and `ENTITYDO` matching API response shape
6. Fill in Doobie Write/Read field mappings for AlloyDB

**REFERENCES:**
- Full worked example: `docs/templates/annotated/congress-amendments-api.md`
- Gold standard: `gov-apis/src/main/scala/congress/gov/apis/LegislativeBillsApi.scala`

---

## 1. Entity Type Enum (skip if no type discriminator)

```scala
package congress.gov.DTOs

case class UnknownENTITYType(value: String)
    extends Exception(s"Unknown ENTITY type: '$value'")

enum ENTITYType(val value: String) {
  case ???  extends ENTITYType("???")   // e.g. case SenateAmendment extends AmendmentType("SAMDT")
  case ???  extends ENTITYType("???")
}

object ENTITYType {
  // Returns Either — never throws. Left propagated by toDO to pipeline fail-and-continue.
  def fromString(value: String): Either[UnknownENTITYType, ENTITYType] = {
    value match {
      case "???" => Right(ENTITYType.???)
      case other => Left(UnknownENTITYType(other))
    }
  }
}
```

---

## 2. GovSite Decoder — maps raw Congress.gov field names to internal names

```scala
package congress.gov.DTOs.GovSite

import java.time.ZonedDateTime
import io.circe.Decoder
import congress.gov.DTOs.ENTITYDTO

object ENTITYDTOGovSite {

  import common.Serializers.*

  implicit val govSiteDecoder: Decoder[ENTITYDTO] =
    (c: io.circe.HCursor) =>
      for {
        congress    <- c.downField("congress").as[Int]
        entity_id   <- c.downField("???").as[String]
        entity_type <- c.downField("???").as[String]
        latestAction <- c.downField("latestAction").as[LatestAction]
        ???         <- c.downField("???").as[???]
        updateDate  <- c.downField("updateDate").as[ZonedDateTime]
        url         <- c.downField("url").as[String]
      } yield ENTITYDTO(
        congress,
        entity_id,
        entity_type,
        latestAction,
        ???,
        updateDate,
        url
      )
}
```

Map API field names → internal field names where they differ. Use `Option[T]` for nullable API fields.

---

## 3. Internal DTO — canonical internal shape

```scala
package congress.gov.DTOs

import java.time.ZonedDateTime

case class ENTITYDTO(
    congress: Int,
    entity_id: String,
    entity_type: String,
    latestAction: LatestAction,
    ???: ???,
    updateDate: ZonedDateTime,
    url: String
) {
  // toDO validates entity_type and returns Either.
  // Left propagates to pipeline fail-and-continue without halting.
  def toDO: Either[String, ENTITYDO] = {
    ENTITYType.fromString(entity_type).map { eType =>
      ENTITYDO(
        congress   = congress,
        entityId   = entity_id,
        entityType = eType,
        latestAction = latestAction,
        ???        = ???,
        updateDate = updateDate,
        url        = org.http4s.Uri.unsafeFromString(url)
      )
    }.left.map(_.getMessage)
  }
}
```

`entity_type` stays String — type validation deferred to `toDO`. GovSite decoding should not fail on unknown type strings.

---

## 4. Internal Circe Codecs

```scala
package congress.gov.DTOs.Internal

import cats.effect.Concurrent
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder}
import org.http4s.EntityDecoder
import congress.gov.DTOs.ENTITYDTO

object ENTITYDTO {
  implicit val decoder: Decoder[ENTITYDTO] = deriveDecoder[ENTITYDTO]
  implicit val encoder: Encoder[ENTITYDTO] = deriveEncoder[ENTITYDTO]

  implicit def entityDecoder[F[_]: Concurrent]: EntityDecoder[F, ENTITYDTO] =
    org.http4s.circe.jsonOf[F, ENTITYDTO]
}
```

---

## 5. Container DTO — wraps a page of results, enables pagination + combining

```scala
package congress.gov.DTOs

import cats.Semigroup
import cats.effect.Concurrent
import org.http4s.EntityDecoder
import apiBase.PagedObject

case class ENTITIESDTO(entities: Seq[ENTITYDTO]) extends PagedObject {
  override def lengthRetrieved: Int = entities.length
}

object ENTITIESDTO {
  implicit val semigroup: Semigroup[ENTITIESDTO] =
    (x: ENTITIESDTO, y: ENTITIESDTO) => ENTITIESDTO(x.entities ++ y.entities)

  implicit val decoder: io.circe.Decoder[ENTITIESDTO] = {
    import io.circe.generic.semiauto.deriveDecoder
    import congress.gov.DTOs.Internal.ENTITYDTO._
    deriveDecoder[ENTITIESDTO]
  }

  implicit def entityDecoder[F[_]: Concurrent]: EntityDecoder[F, ENTITIESDTO] =
    org.http4s.circe.jsonOf[F, ENTITIESDTO]
}
```

Must extend `PagedObject` so `PagingApiBase` knows when to stop fetching pages. Must have `Semigroup` so pages can be combined with `|+|`.

---

## 6. Domain Object — typed fields, Doobie Read/Write for AlloyDB

```scala
package congress.gov.DOs

import java.time.ZonedDateTime
import cats.effect.Async
import doobie.*
import doobie.implicits.*
import doobie.util.transactor.Transactor
import org.http4s.Uri
import org.slf4j.Logger
import congress.gov.DTOs.{ENTITYType, LatestAction}
import pipeline.models.Tables

case class ENTITYDO(
    congress: Int,
    entityId: String,
    entityType: ENTITYType,
    latestActionDate: ZonedDateTime,
    latestActionText: String,
    ???: ???,
    updateDate: ZonedDateTime,
    url: String
) {
  def rowId: String = s"${congress}-${entityType.value}-${entityId}"

  def saveENTITY[F[_]: Async](xa: Transactor[F], logger: Logger): F[Unit] =
    sql"""
      INSERT INTO ${Fragment.const(Tables.???)}
        (entity_id, congress, entity_type, latest_action_date, latest_action_text, update_date, url)
      VALUES
        (${rowId}, ${congress}, ${entityType.value}, ${latestActionDate}, ${latestActionText}, ${updateDate}, ${url})
      ON CONFLICT (entity_id) DO UPDATE
        SET update_date        = EXCLUDED.update_date,
            latest_action_date = EXCLUDED.latest_action_date,
            latest_action_text = EXCLUDED.latest_action_text
    """.update.run.transact(xa).void
}
```

DOs use Doobie auto-derived Read/Write — no toPojo needed. Enum fields require explicit `Meta[ENTITYType]` instance (see enum-with-parsing.md). Option fields map to nullable columns. Natural key mirrors Congress.gov URL structure. Use `Tables` constant — never hardcode table names.

---

## 7. API Client — extends PagingApiBase with entity-specific config

```scala
package congress.gov.apis

import cats.effect.{Async, Resource}
import cats.syntax.all._
import org.http4s.EntityDecoder
import org.http4s.client.Client
import org.http4s.ember.client.EmberClientBuilder
import fs2.io.net.Network
import apiBase.PagingApiBase
import congress.gov.DTOs.ENTITIESDTO

class ENTITYApi[F[_]: Async: Network](
    val protocol: String = "https",
    val host: String = "api.congress.gov",
    val apiKey: String = "DEMO_KEY",
    val path: String = "/v3/???",
    val pageSize: Int = 250
) extends PagingApiBase[F, ENTITIESDTO] {
  override val emberClient: Resource[F, Client[F]] =
    EmberClientBuilder.default[F].build

  override val decoder: EntityDecoder[F, ENTITIESDTO] =
    ENTITIESDTO.entityDecoder
}

object ENTITYApi {
  def apply[F[_]: Async: Network](
      apiKey: String,
      pageSize: Int
  ): F[ENTITYApi[F]] = new ENTITYApi[F](
    apiKey = apiKey,
    pageSize = pageSize
  ).pure[F]
}
```

Extends `PagingApiBase` — all pagination logic inherited. Only provides endpoint-specific configuration.
```