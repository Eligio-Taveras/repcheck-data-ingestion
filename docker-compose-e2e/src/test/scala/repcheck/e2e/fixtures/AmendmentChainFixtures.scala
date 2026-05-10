package repcheck.e2e.fixtures

import io.circe.Json

/**
 * Programmatic generator of Congress.gov amendment fixtures (list + per-amendment detail + text-versions list). Per Q12
 * in §7.7 acceptance criteria — hand-authoring N-deep chains across scenarios doesn't scale. The helper emits the same
 * JSON shapes the real Congress.gov API returns; callers either pass the JSON straight to WireMock's admin API for
 * runtime stubbing, or write the result to `e2e/wiremock/__files/amendments/` for file-loaded mappings.
 *
 * Two responsibilities, kept separate so callers can use whichever:
 *
 *   - `listResponse` — the `/v3/amendment` list envelope. One entry per amendment in `chain`. Each entry's `url`
 *     references the WireMock-served detail path so the pipeline's HATEOAS-follow lands in WireMock.
 *
 *   - `detailResponse` — the `/v3/amendment/{c}/{type}/{n}` envelope for ONE amendment. Includes `amendedBill` when the
 *     parent is a bill (chain head pointing at the root bill) or `amendedAmendment` when the parent is another
 *     amendment (sub-amendments deeper in the chain).
 *
 *   - `textVersionsResponse` — the `/v3/amendment/{c}/{type}/{n}/text` envelope. Caller supplies the set of
 *     `(versionType, formatType, url)` tuples for the amendment; the helper assembles the response. Empty list maps to
 *     pagination.count = 0 so the §7.5 checker treats it as "no text" and emits no event.
 *
 * Out of scope here:
 *   - WireMock mapping JSON. Mappings live under `e2e/wiremock/mappings/` and are static (one per amendment) — they
 *     don't vary per scenario.
 *   - Sub-amendment chain head/leaf semantics. Caller composes the parent relationships explicitly by passing `parent =
 *     Bill(...)` or `parent = Amendment(...)`.
 *
 * Mirror of bills-side `e2e/wiremock/__files/.raw/abridge.py` in concept (programmatically produce the JSON), but lives
 * in Scala so the cross-pipeline spec can vary the chain shape per scenario without dropping into Python.
 */
object AmendmentChainFixtures {

  /** Reference to the bill an amendment directly amends. */
  final case class BillRef(
    congress: Int,
    billType: String,
    number: String,
    title: String,
    originChamber: String = "House",
    originChamberCode: String = "H",
  ) {

    private[fixtures] def asJson(wiremockBase: String): Json = Json.obj(
      "congress"          -> Json.fromInt(congress),
      "number"            -> Json.fromString(number),
      "originChamber"     -> Json.fromString(originChamber),
      "originChamberCode" -> Json.fromString(originChamberCode),
      "title"             -> Json.fromString(title),
      "type"              -> Json.fromString(billType),
      "url"               -> Json.fromString(s"$wiremockBase/v3/bill/$congress/${billType.toLowerCase}/$number"),
    )

  }

  /** Reference to a parent amendment (when the current amendment amends another amendment, not a bill). */
  final case class AmendmentRef(
    congress: Int,
    amendmentType: String,
    number: String,
    purpose: String,
    updateDate: String,
  ) {

    private[fixtures] def asJson(wiremockBase: String): Json = Json.obj(
      "congress"   -> Json.fromInt(congress),
      "number"     -> Json.fromString(number),
      "type"       -> Json.fromString(amendmentType),
      "purpose"    -> Json.fromString(purpose),
      "updateDate" -> Json.fromString(updateDate),
      "url" -> Json.fromString(
        s"$wiremockBase/v3/amendment/$congress/${amendmentType.toLowerCase}/$number?format=json"
      ),
    )

  }

  /** Sealed alternative for an amendment's parent. */
  sealed trait Parent

  object Parent {
    final case class Bill(ref: BillRef)           extends Parent
    final case class Amendment(ref: AmendmentRef) extends Parent
  }

  /** Sponsor row (one entry in the `sponsors` array). */
  final case class Sponsor(
    bioguideId: String,
    firstName: String,
    lastName: String,
    fullName: String,
    party: String,
    state: String,
  ) {

    private[fixtures] def asJson: Json = Json.obj(
      "bioguideId" -> Json.fromString(bioguideId),
      "firstName"  -> Json.fromString(firstName),
      "lastName"   -> Json.fromString(lastName),
      "fullName"   -> Json.fromString(fullName),
      "party"      -> Json.fromString(party),
      "state"      -> Json.fromString(state),
    )

  }

  /** One amendment's complete spec. `parent` is the bill or amendment it amends. */
  final case class AmendmentSpec(
    congress: Int,
    amendmentType: String,
    number: String,
    description: String,
    purpose: String,
    chamber: String,
    sponsor: Sponsor,
    parent: Parent,
    updateDate: String,
    submittedDate: String,
    proposedDate: String,
    latestActionDate: String,
    latestActionText: String,
    latestActionTime: String,
  )

  /** One text-version entry on the `/text` response (one per (versionType, formatType, url) tuple). */
  final case class TextVersionEntry(
    versionType: String,
    date: String,
    formatType: String,
    url: String,
  )

  /**
   * Build the `/v3/amendment` list envelope. Every amendment in `chain` becomes one item; `url` points at the
   * WireMock-served detail path (so `AmendmentsApiClient.fetchDetail` follows the HATEOAS link back to WireMock).
   */
  def listResponse(chain: List[AmendmentSpec], wiremockBase: String): Json = {
    val items = chain.map { spec =>
      Json.obj(
        "congress"    -> Json.fromInt(spec.congress),
        "number"      -> Json.fromString(spec.number),
        "type"        -> Json.fromString(spec.amendmentType),
        "description" -> Json.fromString(spec.description),
        "updateDate"  -> Json.fromString(spec.updateDate),
        "url" -> Json.fromString(
          s"$wiremockBase/v3/amendment/${spec.congress}/${spec.amendmentType.toLowerCase}/${spec.number}?format=json"
        ),
      )
    }
    Json.obj(
      "amendments" -> Json.arr(items*),
      "pagination" -> Json.obj("count" -> Json.fromInt(items.size)),
    )
  }

  /**
   * Build the `/v3/amendment/{c}/{type}/{n}` detail envelope for ONE amendment. Includes either `amendedBill` (when the
   * parent is a bill) or `amendedAmendment` (when the parent is another amendment). The `wiremockBase` is the URL
   * prefix WireMock serves on (e.g. `http://wiremock:8080`) — used for any nested URLs so they all loop back into the
   * stub stack rather than hitting the real api.congress.gov.
   */
  def detailResponse(spec: AmendmentSpec, wiremockBase: String): Json = {
    val parentField = spec.parent match {
      case Parent.Bill(ref)      => "amendedBill"      -> ref.asJson(wiremockBase)
      case Parent.Amendment(ref) => "amendedAmendment" -> ref.asJson(wiremockBase)
    }
    val amendmentObj = Json.obj(
      "congress" -> Json.fromInt(spec.congress),
      "number"   -> Json.fromString(spec.number),
      "type"     -> Json.fromString(spec.amendmentType),
      parentField,
      "chamber"       -> Json.fromString(spec.chamber),
      "description"   -> Json.fromString(spec.description),
      "purpose"       -> Json.fromString(spec.purpose),
      "sponsors"      -> Json.arr(spec.sponsor.asJson),
      "submittedDate" -> Json.fromString(spec.submittedDate),
      "proposedDate"  -> Json.fromString(spec.proposedDate),
      "latestAction" -> Json.obj(
        "actionDate" -> Json.fromString(spec.latestActionDate),
        "text"       -> Json.fromString(spec.latestActionText),
        "actionTime" -> Json.fromString(spec.latestActionTime),
      ),
      "updateDate" -> Json.fromString(spec.updateDate),
    )
    Json.obj("amendment" -> amendmentObj)
  }

  /**
   * Build the `/v3/amendment/{c}/{type}/{n}/text` envelope. `entries` groups by (versionType, date) under the hood so
   * the same source granule can carry multiple format URLs (the §7.5 checker selects one (versionType, formatType)
   * tuple per emit, but the response shape is one `formats[]` per `textVersions[]` entry).
   */
  def textVersionsResponse(entries: List[TextVersionEntry]): Json = {
    val grouped = entries.groupBy(e => (e.versionType, e.date)).toList.sortBy { case ((t, d), _) => (t, d) }
    val versions = grouped.map {
      case ((versionType, date), formats) =>
        Json.obj(
          "type" -> Json.fromString(versionType),
          "date" -> Json.fromString(date),
          "formats" -> Json.arr(
            formats.map(e => Json.obj("type" -> Json.fromString(e.formatType), "url" -> Json.fromString(e.url)))*
          ),
        )
    }
    Json.obj(
      "textVersions" -> Json.arr(versions*),
      "pagination"   -> Json.obj("count" -> Json.fromInt(versions.size)),
    )
  }

  /**
   * Convenience: stringify any Circe Json result with stable, pretty formatting. Useful for callers that want to
   * compare against a checked-in fixture file or write the result to disk for WireMock to load.
   */
  def pretty(json: Json): String = json.spaces2

  /**
   * Sample chain used by [[repcheck.e2e.AmendmentsCrossPipelineSpec]]. Captured here (rather than inline in the spec)
   * so the chain shape is self-documenting and reusable across scenarios. Bill = `117-HR-3684`; SAMDT 100 amends the
   * bill; SAMDT 101 fan-out also amends the bill; SUAMDT 200 amends SAMDT 100. Mirror of the fixture JSON files already
   * checked into `e2e/wiremock/__files/amendments/` — the helper is the SPEC, the JSON files are the MATERIALIZATION
   * (kept on disk so WireMock can load mappings statically at container startup).
   */
  val sampleBill: BillRef = BillRef(
    congress = 117,
    billType = "HR",
    number = "3684",
    title = "Infrastructure Investment and Jobs Act",
  )

  val sampleSamdt100: AmendmentSpec = AmendmentSpec(
    congress = 117,
    amendmentType = "SAMDT",
    number = "100",
    description = "E2E SAMDT 100 — root amendment in the chain",
    purpose = "To improve the bill",
    chamber = "Senate",
    sponsor = Sponsor("S001217", "Jeanne", "Shaheen", "Sen. Shaheen, Jeanne [D-NH]", "D", "NH"),
    parent = Parent.Bill(sampleBill),
    updateDate = "2024-06-01T12:00:00Z",
    submittedDate = "2021-08-01",
    proposedDate = "2021-08-02",
    latestActionDate = "2024-06-01",
    latestActionText = "Amendment SA 100 agreed to in Senate by Voice Vote.",
    latestActionTime = "12:00:00",
  )

  val sampleSamdt101: AmendmentSpec = AmendmentSpec(
    congress = 117,
    amendmentType = "SAMDT",
    number = "101",
    description = "E2E SAMDT 101 — fan-out amendment to the same bill",
    purpose = "To clarify section 5",
    chamber = "Senate",
    sponsor = Sponsor("C000174", "Tom", "Carper", "Sen. Carper, Tom [D-DE]", "D", "DE"),
    parent = Parent.Bill(sampleBill),
    updateDate = "2024-06-01T12:30:00Z",
    submittedDate = "2021-08-01",
    proposedDate = "2021-08-02",
    latestActionDate = "2024-06-01",
    latestActionText = "Amendment SA 101 agreed to in Senate by Voice Vote.",
    latestActionTime = "12:30:00",
  )

  val sampleSuamdt200: AmendmentSpec = AmendmentSpec(
    congress = 117,
    amendmentType = "SUAMDT",
    number = "200",
    description = "E2E SUAMDT 200 — sub-amendment to SAMDT 100",
    purpose = "To narrow the scope",
    chamber = "Senate",
    sponsor = Sponsor("M000133", "Edward", "Markey", "Sen. Markey, Edward [D-MA]", "D", "MA"),
    parent = Parent.Amendment(
      AmendmentRef(
        congress = 117,
        amendmentType = "SAMDT",
        number = "100",
        purpose = "To improve the bill",
        updateDate = "2024-06-01T12:00:00Z",
      )
    ),
    updateDate = "2024-06-02T13:00:00Z",
    submittedDate = "2021-08-03",
    proposedDate = "2021-08-04",
    latestActionDate = "2024-06-02",
    latestActionText = "Amendment SU 200 agreed to in Senate by Voice Vote.",
    latestActionTime = "13:00:00",
  )

  /**
   * Sample chain in canonical list-order — used by [[AmendmentsCrossPipelineSpec]] for both list assertions and detail
   * assertions.
   */
  val sampleChain: List[AmendmentSpec] = List(sampleSamdt100, sampleSuamdt200, sampleSamdt101)

}
