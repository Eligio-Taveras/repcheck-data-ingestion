package repcheck.ingestion.votes.app

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Scaffold-level smoke test that verifies the votes-pipeline subproject compiles and its top-level symbols resolve to
 * the expected fully-qualified names. Behavior-level tests arrive in Phase 2 PRs once the API clients, repositories,
 * resolver, and processor land.
 */
class VotesPipelineScaffoldSpec extends AnyFlatSpec with Matchers {

  "VotesPipelineApp" should "resolve to the expected fully-qualified name" in {
    VotesPipelineApp.getClass.getName shouldBe "repcheck.ingestion.votes.app.VotesPipelineApp$"
  }

  "VotesPipeline.AppConfig" should "be a case class under the votes-pipeline app package" in {
    classOf[VotesPipeline.AppConfig].getName shouldBe
      "repcheck.ingestion.votes.app.VotesPipeline$AppConfig"
  }

}
