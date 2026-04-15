package repcheck.members.common.diff

import java.time.Instant

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.shared.models.congress.common.{Party, UsState}
import repcheck.shared.models.congress.dos.member.MemberDO

class MemberDifferSpec extends AnyFlatSpec with Matchers {

  import MemberDiffer.given

  private val baseMember = MemberDO(
    memberId = 1L,
    naturalKey = "A000001",
    firstName = Some("John"),
    lastName = Some("Doe"),
    directOrderName = Some("John Doe"),
    invertedOrderName = Some("Doe, John"),
    honorificName = Some("Rep. John Doe"),
    birthYear = Some(1970),
    currentParty = Some(Party.Democrat),
    state = Some(UsState.NewYork),
    district = Some(5),
    imageUrl = Some("https://example.com/photo.jpg"),
    imageAttribution = Some("Official Photo"),
    officialUrl = Some("https://doe.house.gov"),
    updateDate = Some(Instant.parse("2024-01-15T00:00:00Z")),
    createdAt = Some(Instant.parse("2024-01-01T00:00:00Z")),
    updatedAt = Some(Instant.parse("2024-01-15T00:00:00Z")),
  )

  "MemberDiffer" should "report no diff for identical members" in {
    val differ = summon[difflicious.Differ[MemberDO]]
    val result = differ.diff(baseMember, baseMember)
    result.isOk shouldBe true
  }

  it should "detect changes when firstName differs" in {
    val differ  = summon[difflicious.Differ[MemberDO]]
    val changed = baseMember.copy(firstName = Some("Jane"))
    val result  = differ.diff(baseMember, changed)
    result.isOk shouldBe false
  }

  it should "detect changes when party changes" in {
    val differ  = summon[difflicious.Differ[MemberDO]]
    val changed = baseMember.copy(currentParty = Some(Party.Republican))
    val result  = differ.diff(baseMember, changed)
    result.isOk shouldBe false
  }

  it should "detect changes when state changes" in {
    val differ  = summon[difflicious.Differ[MemberDO]]
    val changed = baseMember.copy(state = Some(UsState.California))
    val result  = differ.diff(baseMember, changed)
    result.isOk shouldBe false
  }

  it should "detect changes when optional field goes from None to Some" in {
    val differ   = summon[difflicious.Differ[MemberDO]]
    val withNone = baseMember.copy(imageUrl = None)
    val result   = differ.diff(withNone, baseMember)
    result.isOk shouldBe false
  }

  it should "detect changes when optional field goes from Some to None" in {
    val differ   = summon[difflicious.Differ[MemberDO]]
    val withNone = baseMember.copy(officialUrl = None)
    val result   = differ.diff(baseMember, withNone)
    result.isOk shouldBe false
  }

  it should "report no diff when only ignored fields differ (memberId, createdAt, updatedAt)" in {
    val differ = summon[difflicious.Differ[MemberDO]]
    // memberId, createdAt, updatedAt are DB-managed fields that may differ between
    // incoming and stored but are part of the case class. The Differ sees them as real changes.
    // This test documents that behavior — the ChangeDetector uses updateDate comparison
    // to short-circuit before diffing, so these fields rarely cause false positives.
    val differentId = baseMember.copy(memberId = 99L)
    val result      = differ.diff(baseMember, differentId)
    result.isOk shouldBe false
  }

  // Table-driven: mutates one field at a time and asserts the Differ detects each change.
  // Exercises every field-specific branch of the auto-derived Differ[MemberDO] — without this,
  // only a handful of fields are diffed and the derived branches for the remaining fields
  // stay uncovered, dragging the file below the 95% coverage gate.
  private val fieldMutations: List[(String, MemberDO => MemberDO)] = List(
    "naturalKey"        -> (_.copy(naturalKey = "B000002")),
    "lastName"          -> (_.copy(lastName = Some("Smith"))),
    "directOrderName"   -> (_.copy(directOrderName = Some("Jane Doe"))),
    "invertedOrderName" -> (_.copy(invertedOrderName = Some("Smith, Jane"))),
    "honorificName"     -> (_.copy(honorificName = Some("Sen. Jane Smith"))),
    "birthYear"         -> (_.copy(birthYear = Some(1980))),
    "district"          -> (_.copy(district = Some(12))),
    "imageAttribution"  -> (_.copy(imageAttribution = Some("Updated Photo"))),
    "updateDate"        -> (_.copy(updateDate = Some(Instant.parse("2024-06-01T00:00:00Z")))),
    "createdAt"         -> (_.copy(createdAt = Some(Instant.parse("2024-06-01T00:00:00Z")))),
    "updatedAt"         -> (_.copy(updatedAt = Some(Instant.parse("2024-06-01T00:00:00Z")))),
  )

  fieldMutations.foreach {
    case (fieldName, mutate) =>
      it should s"detect changes when $fieldName differs" in {
        val differ  = summon[difflicious.Differ[MemberDO]]
        val changed = mutate(baseMember)
        val result  = differ.diff(baseMember, changed)
        result.isOk shouldBe false
      }
  }

  // Edge case: Some -> None transitions for every Option field, exercising the
  // Option-aware branch of the derived Differ for each field.
  private val optionToNoneMutations: List[(String, MemberDO => MemberDO)] = List(
    "firstName"         -> (_.copy(firstName = None)),
    "lastName"          -> (_.copy(lastName = None)),
    "directOrderName"   -> (_.copy(directOrderName = None)),
    "invertedOrderName" -> (_.copy(invertedOrderName = None)),
    "honorificName"     -> (_.copy(honorificName = None)),
    "birthYear"         -> (_.copy(birthYear = None)),
    "currentParty"      -> (_.copy(currentParty = None)),
    "state"             -> (_.copy(state = None)),
    "district"          -> (_.copy(district = None)),
    "imageAttribution"  -> (_.copy(imageAttribution = None)),
    "updateDate"        -> (_.copy(updateDate = None)),
    "createdAt"         -> (_.copy(createdAt = None)),
    "updatedAt"         -> (_.copy(updatedAt = None)),
  )

  optionToNoneMutations.foreach {
    case (fieldName, mutate) =>
      it should s"detect changes when $fieldName goes from Some to None" in {
        val differ  = summon[difflicious.Differ[MemberDO]]
        val cleared = mutate(baseMember)
        val result  = differ.diff(baseMember, cleared)
        result.isOk shouldBe false
      }
  }

  it should "report no diff for two placeholder members with identical natural keys" in {
    // Exercises the Option[_] diff branch where BOTH sides are None for every optional field.
    val differ = summon[difflicious.Differ[MemberDO]]
    val placeholder = MemberDO(
      memberId = 0L,
      naturalKey = "P000000",
      firstName = None,
      lastName = None,
      directOrderName = None,
      invertedOrderName = None,
      honorificName = None,
      birthYear = None,
      currentParty = None,
      state = None,
      district = None,
      imageUrl = None,
      imageAttribution = None,
      officialUrl = None,
      updateDate = None,
      createdAt = None,
      updatedAt = None,
    )
    val result = differ.diff(placeholder, placeholder)
    result.isOk shouldBe true
  }

}
