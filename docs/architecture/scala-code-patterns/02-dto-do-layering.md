> Part of [Scala Code Patterns](../SCALA_CODE_PATTERNS.md)

## 2. DTO / DO Layering

**Pattern**: Strict separation between external data shapes (DTOs) and business domain objects (DOs). Every external boundary has its own DTO.

### Three DTO Types

```
Congress.gov JSON  →  ApiDTO  →  DO (domain logic)
Pub/Sub message    →  EventDTO → (used directly, lives in pipeline-models)
```

### Naming Convention

| Type | Suffix | Location | Example |
|---|---|---|---|
| API response shape | `ApiDTO` | Same repo `models/` project | `LegislativeBillApiDTO` |
| Domain business object | `DO` | Same repo `models/` project | `LegislativeBillDO` |
| Pub/Sub event | `Event` | `repcheck-pipeline-models` | `BillTextAvailableEvent` |

### Conversion Methods

DTOs always convert **to** DOs via a `.toDO()` method on the DTO. DOs convert **to** DTOs via a companion `fromDO()` on the DTO.

```scala
// API DTO — matches Congress.gov JSON structure exactly
case class LegislativeBillApiDTO(
  congress: Int,
  number: String,
  `type`: String,
  title: String,
  originChamber: String,
  latestAction: LatestActionApiDTO,
  updateDate: String,
  url: String
) {
  def toDO(): LegislativeBillDO = LegislativeBillDO(
    billId = s"${`type`.toLowerCase}-$number-$congress",
    congress = congress,
    billType = BillType.fromString(`type`),
    title = title,
    originChamber = Chamber.fromString(originChamber),
    latestActionText = latestAction.text,
    latestActionDate = latestAction.actionDate,
    updateDate = ZonedDateTime.parse(updateDate),
    url = url
  )
}

// DB DTO — matches AlloyDB table structure. Used for reading from AlloyDB.
case class LegislativeBillDbDTO(
  billId: String,
  congress: Long,
  billType: String,
  title: String,
  originChamber: String,
  latestActionText: String,
  latestActionDate: Timestamp,
  updateDate: Timestamp,
  url: String,
  textUrl: Option[String]
) {
  def toDO(): LegislativeBillDO = LegislativeBillDO(
    billId = billId,
    congress = congress.toInt,
    billType = BillType.fromString(billType),
    title = title,
    originChamber = Chamber.fromString(originChamber),
    latestActionText = latestActionText,
    latestActionDate = latestActionDate.toDate.toInstant.atZone(ZoneOffset.UTC),
    updateDate = updateDate.toDate.toInstant.atZone(ZoneOffset.UTC),
    url = url,
    textUrl = textUrl
  )
}

object LegislativeBillDbDTO {
  def fromDO(bill: LegislativeBillDO): LegislativeBillDbDTO = LegislativeBillDbDTO(
    billId = bill.billId,
    congress = bill.congress.toLong,
    billType = bill.billType.value,
    title = bill.title,
    originChamber = bill.originChamber.value,
    latestActionText = bill.latestActionText,
    latestActionDate = Timestamp.ofTimeSecondsAndNanos(
      bill.latestActionDate.toEpochSecond, 0
    ),
    updateDate = Timestamp.ofTimeSecondsAndNanos(
      bill.updateDate.toEpochSecond, 0
    ),
    url = bill.url,
    textUrl = bill.textUrl
  )
}

// Domain Object — business-typed, no serialization concerns
case class LegislativeBillDO(
  billId: String,
  congress: Int,
  billType: BillType,
  title: String,
  originChamber: Chamber,
  latestActionText: String,
  latestActionDate: ZonedDateTime,
  updateDate: ZonedDateTime,
  url: String,
  textUrl: Option[String] = None
)
```

### Rules
- **DTOs** contain only primitive types and other DTOs — no domain enums, no business logic
- **DOs** use domain-typed fields (enums, value classes, `ZonedDateTime`, etc.)
- DTOs and DOs for a service live in that repo's `models/` sub-project, published as a library
- **Never** pass DTOs across module boundaries — always convert to DO first
- API DTOs have Circe decoders; DB DTOs have Doobie auto-derived `Read`/`Write` instances
- DOs have no serialization — they are pure domain types
