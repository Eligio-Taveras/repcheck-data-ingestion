<!-- GENERATED FILE — DO NOT EDIT. Source: docs/templates/annotated/circe-codecs.md -->

# Pattern: Circe Codecs

## When To Use This Pattern
- Any DTO needing JSON serialization/deserialization
- Custom types (dates, enums, URIs) not natively supported by Circe
- Bridging external API field names to internal naming conventions

## Approach 1: Semi-Auto Derivation (Internal DTOs)

Use when field names in JSON match case class field names exactly.

```scala
// File: gov-apis/src/main/scala/congress/gov/DTOs/Internal/LegislativeBillDTO.scala

package congress.gov.DTOs.Internal

import cats.effect.Concurrent

import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.{Decoder, Encoder}

import org.http4s.EntityDecoder

import congress.gov.DTOs.LegislativeBillDTO

object LegislativeBillDTO {
  // Semi-auto derivation generates encoder/decoder from case class fields.
  // Field names in JSON must EXACTLY match case class field names.
  // Semi-auto (not full auto) makes implicit scope explicit.
  implicit val decoder: Decoder[LegislativeBillDTO] =
    deriveDecoder[LegislativeBillDTO]
  implicit val encoder: Encoder[LegislativeBillDTO] =
    deriveEncoder[LegislativeBillDTO]

  // http4s bridge — creates EntityDecoder from Circe Decoder.
  // Lets http4s client.expect[T](uri) automatically parse JSON responses.
  implicit def entityDecoder[F[_]: Concurrent]
  : EntityDecoder[F, LegislativeBillDTO] =
    org.http4s.circe.jsonOf[F, LegislativeBillDTO]
}
```

## Approach 2: Manual Decoder (External API Field Mapping)

Use when external API field names don't match your case class.

```scala
// File: gov-apis/src/main/scala/congress/gov/DTOs/GovSite/LegislativeBillDTOGovSite.scala

package congress.gov.DTOs.GovSite

import java.time.{LocalDate, ZonedDateTime}

import io.circe.Decoder

import congress.gov.DTOs.{LatestAction, LegislativeBillDTO}

object LegislativeBillDTOGovSite {

  import common.Serializers.*

  // Manual HCursor decoder for field name mapping. Congress.gov API returns "number" → maps to bill_id, "type" → bill_type.
  // c.downField navigates JSON structure; for-comprehension fails on first decode error with descriptive message.
  implicit val govSiteDecoder: Decoder[LegislativeBillDTO] =
    (c: io.circe.HCursor) =>
      for {
        congress <- c.downField("congress").as[Int]
        bill_id <- c.downField("number").as[String]     // MAPPING: "number" → bill_id
        bill_type <- c.downField("type").as[String]      // MAPPING: "type" → bill_type
        latestAction <- c.downField("latestAction").as[LatestAction]
        originChamber <- c.downField("originChamber").as[String]
        originChamberCode <- c.downField("originChamberCode").as[String]
        title <- c.downField("title").as[String]
        updateDate <- c.downField("updateDate").as[LocalDate]
        updateDateIncludingText <- c
          .downField("updateDateIncludingText")
          .as[ZonedDateTime]
        url <- c.downField("url").as[String]
      } yield LegislativeBillDTO(
        congress, bill_id, bill_type, latestAction,
        originChamber, originChamberCode, title,
        updateDate, updateDateIncludingText, url
      )
}
```

## Approach 3: Custom Codecs for Domain Types

Use for types that aren't natively supported (dates, enums, URIs).

```scala
// File: gov-apis/src/main/scala/common/Serializers.scala

package common

import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter

import io.circe.{Decoder, Encoder}

object Serializers {
  // Custom ZonedDateTime decoder. Circe lacks built-in support due to format variation.
  // Decoder.decodeString.emap: decode JSON as String, then transform String → Either[String, T].
  // Left message becomes decoder error.
  implicit val zonedDateTimeDecoder: Decoder[ZonedDateTime] =
    Decoder.decodeString.emap { str =>
      scala.util
        .Try {
          ZonedDateTime.parse(str, DateTimeFormatter.ISO_ZONED_DATE_TIME)
        }
        .toEither
        .left
        .map(error => {
          "Failed to decode datetime: " + error.getMessage
        })
    }

  // Custom ZonedDateTime encoder. Encoder.encodeString.contramap: transform input ZonedDateTime → String before encoding.
  implicit val zonedDateTimeEncoder: Encoder[ZonedDateTime] =
    Encoder.encodeString.contramap[ZonedDateTime](dt =>
      val strVal = dt.format(DateTimeFormatter.ISO_ZONED_DATE_TIME)
      strVal
    )
}
```

## Approach 4: Custom Encoder Reshaping Fields

Use when the serialized JSON shape differs from the case class shape.

```scala
// File: gov-apis/src/main/scala/congress/gov/DTOs/LatestAction.scala

package congress.gov.DTOs

import java.time.{LocalDate, LocalTime, ZoneId, ZonedDateTime}
import scala.jdk.CollectionConverters._
import io.circe.syntax._
import io.circe.{Decoder, Encoder, HCursor, Json}

case class LatestAction(actionDate: ZonedDateTime, text: String)

object LatestAction {
  // Custom encoder SPLITS single ZonedDateTime into two JSON fields ("actionDate", "actionTime").
  // Matches Congress.gov API format. Json.obj creates JSON object; .asJson uses Circe encoder for each value.
  implicit val encoder: Encoder[LatestAction] = (a: LatestAction) => {
    Json.obj(
      ("actionDate", a.actionDate.toLocalDate.asJson),
      ("actionTime", a.actionDate.toLocalTime.asJson),
      ("text", Json.fromString(a.text))
    )
  }

  // Custom decoder COMBINES two JSON fields into one ZonedDateTime.
  // "actionTime" is optional; .fold provides midnight default if absent.
  implicit val decoder: Decoder[LatestAction] = (c: HCursor) => {
    for {
      actionLocalDate <- c.downField("actionDate").as[LocalDate]
      actionLocalTime <- c.downField("actionTime").as[Option[LocalTime]]
      text <- c.downField("text").as[String]
    } yield {
      val actionTime = actionLocalTime.fold(LocalTime.of(0, 0, 0))(identity)
      val actionDate =
        ZonedDateTime.of(actionLocalDate, actionTime, ZoneId.of("UTC"))
      LatestAction(actionDate, text)
    }
  }
}
```

## Decision Guide

| Situation | Approach | Import |
|-----------|----------|--------|
| Field names match exactly | Semi-auto derivation | `io.circe.generic.semiauto._` |
| External API field names differ | Manual HCursor decoder | `io.circe.Decoder` |
| Custom types (dates, enums) | `emap` / `contramap` | `io.circe.{Decoder, Encoder}` |
| JSON shape differs from case class | Manual `Json.obj` encoder | `io.circe.{Encoder, Json}` |
| http4s integration | `jsonOf[F, T]` EntityDecoder | `org.http4s.circe.jsonOf` |

## How to Create Codecs for a New DTO

```scala
// For a new VoteDTO with matching field names:
object VoteDTO {
  implicit val decoder: Decoder[VoteDTO] = deriveDecoder[VoteDTO]
  implicit val encoder: Encoder[VoteDTO] = deriveEncoder[VoteDTO]
  implicit def entityDecoder[F[_]: Concurrent]: EntityDecoder[F, VoteDTO] =
    org.http4s.circe.jsonOf[F, VoteDTO]
}

// For a GovSite decoder with different field names:
object VoteDTOGovSite {
  implicit val decoder: Decoder[VoteDTO] = (c: HCursor) =>
    for {
      voteId    <- c.downField("roll_call").as[String]   // API: roll_call → voteId
      chamber   <- c.downField("chamber").as[String]
      result    <- c.downField("result").as[String]
    } yield VoteDTO(voteId, chamber, result)
}
```