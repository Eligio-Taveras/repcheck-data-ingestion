package repcheck.ingestion.votes.pipeline

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.shared.models.congress.common.{Party, UsState}
import repcheck.shared.models.congress.dos.results.UnresolvedVotePosition
import repcheck.shared.models.congress.vote.VoteCast

/**
 * Unit spec for the pure position builders extracted into [[VotePositionBuilders]]. Verifies that each builder only
 * materializes rows for its own arm (House = `memberSource = Left`, Senate = `memberSource = Right`) and correctly maps
 * bioguide / LIS-natural-key to the pre-resolved surrogate id.
 */
class VotePositionBuildersSpec extends AnyFlatSpec with Matchers {

  private def housePos(bioguide: String, cast: VoteCast = VoteCast.Yea): UnresolvedVotePosition =
    UnresolvedVotePosition(
      memberSource = Left(bioguide),
      voteCast = Some(cast),
      partyAtVote = Some(Party.Democrat),
      stateAtVote = Some(UsState.NewYork),
    )

  private def senatePos(lisId: String, cast: VoteCast = VoteCast.Yea): UnresolvedVotePosition =
    UnresolvedVotePosition(
      memberSource = Right(lisId),
      voteCast = Some(cast),
      partyAtVote = Some(Party.Democrat),
      stateAtVote = Some(UsState.Maryland),
    )

  "house" should "materialize House-arm VotePositionDOs with the given voteId + resolved member_ids" in {
    val positions = List(
      housePos("A000055", VoteCast.Yea),
      senatePos("S428", VoteCast.Nay), // Senate entry — dropped by House builder
    )
    val result = VotePositionBuilders.house(42L, positions, Map("A000055" -> 7L))

    val _ = result.length shouldBe 1
    val p = result.headOption.getOrElse(fail("expected one position"))
    val _ = p.voteId shouldBe 42L
    val _ = p.memberId shouldBe Some(7L)
    val _ = p.lisMemberId shouldBe None
    val _ = p.position shouldBe Some(VoteCast.Yea)
    val _ = p.partyAtVote shouldBe Some(Party.Democrat)
    p.stateAtVote shouldBe Some(UsState.NewYork)
  }

  it should "drop House positions whose bioguide is absent from the memberMap (defensive)" in {
    val result = VotePositionBuilders.house(42L, List(housePos("A000055"), housePos("B000999")), Map("A000055" -> 7L))
    val _      = result.length shouldBe 1
    result.headOption.map(_.memberId) shouldBe Some(Some(7L))
  }

  it should "return an empty list when the position input is empty" in {
    VotePositionBuilders.house(42L, List.empty, Map.empty) shouldBe List.empty
  }

  "senate" should "materialize Senate-arm VotePositionDOs with the given voteId + resolved lis_members.id" in {
    val positions = List(
      senatePos("S428", VoteCast.Yea),
      housePos("A000055", VoteCast.Nay), // House entry — dropped by Senate builder
    )
    val result = VotePositionBuilders.senate(88L, positions, Map("S428" -> 99L))

    val _ = result.length shouldBe 1
    val p = result.headOption.getOrElse(fail("expected one position"))
    val _ = p.voteId shouldBe 88L
    val _ = p.memberId shouldBe None
    val _ = p.lisMemberId shouldBe Some(99L)
    val _ = p.position shouldBe Some(VoteCast.Yea)
    p.stateAtVote shouldBe Some(UsState.Maryland)
  }

  it should "drop Senate positions whose LIS natural key is absent from the lisMap (defensive)" in {
    val result = VotePositionBuilders.senate(88L, List(senatePos("S428"), senatePos("S999")), Map("S428" -> 99L))
    val _      = result.length shouldBe 1
    result.headOption.map(_.lisMemberId) shouldBe Some(Some(99L))
  }

  it should "return an empty list when the position input is empty" in {
    VotePositionBuilders.senate(88L, List.empty, Map.empty) shouldBe List.empty
  }

}
