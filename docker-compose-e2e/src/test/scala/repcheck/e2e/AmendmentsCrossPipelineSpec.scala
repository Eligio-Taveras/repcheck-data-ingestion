package repcheck.e2e

import scala.annotation.tailrec

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import doobie.implicits._
import doobie.util.transactor.Transactor

import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.ingestion.bills.common.testing.DockerRequired

/**
 * Cross-pipeline E2E for the §7 amendments stack. Brings up [[docker-compose.e2e.yml]] via [[ComposeStackFixture]],
 * runs the bills + members baseline (so `bills`/`members` have rows the amendments side can FK to and the §S6
 * regression has its data) followed by the three new amendments pipelines, then asserts on the resulting AlloyDB state.
 *
 * Scenario coverage (per IMPLEMENTATION_PLAN §6.6):
 *
 *   - S1 — Chain happy path. Bill → SAMDT 100 → SUAMDT 200 all hydrate via inline recursion. SAMDT 101 fan-out proves
 *     the same bill anchors multiple amendments.
 *   - S3 — Fan-out. One bill (117-HR-3684), two SAMDTs (100 + 101) — both rows share `bill_id` after the chain
 *     resolves. SUAMDT 200's `bill_id` is the resolved ancestor (the same bill via SAMDT 100), not its direct parent.
 *   - S4 — Text ingestion. Availability checker publishes one `AmendmentTextAvailableEvent` (only SAMDT 100 has a text
 *     granule in the fixture); the amendment-text-pipeline subscriber consumes it, fetches the granule via the GovInfo
 *     URL rewriter, chunks + embeds (against the WireMock-stubbed Ollama endpoint), and writes
 *     `amendment_text_versions.fetched_at IS NOT NULL` + at least one `amendment_text_chunks` row.
 *   - S6 — Backwards-compat regression. The existing [[DockerComposeE2EStackSpec]] continues to pass its pre-amendments
 *     assertions; this spec adds amendments on top of the same bills/members baseline and asserts bill rows weren't
 *     disturbed (sanity-check; the full regression lives in the existing spec).
 *
 * Out of scope for this spec (deferred to follow-on PRs — see PR description):
 *   - S2 (votes-pipeline placeholder upgrade). Requires a Senate amendment-vote XML fixture and votes-pipeline run
 *     before the amendments-pipeline run. The wiring exists in code (`SenateVoteConverter`'s amendment branch from
 *     §7.4) but the e2e ordering + fixture work is its own change.
 *   - S5 failure modes. WireMock stateful-stub support (`scenarios`) for the 5xx-then-200 path, fault-injection for
 *     crash recovery, and Ollama-down simulation — each its own scenario PR.
 *
 * Tagged [[DockerRequired]] so the default `sbt test` skips it. The `e2e-gcp` CI job runs it via:
 * {{{
 *   sbt 'set dockerComposeE2e / Test / testOptions := Seq(
 *         Tests.Argument(TestFrameworks.ScalaTest, "-n", "DockerRequired"))' \
 *       'dockerComposeE2e/test'
 * }}}
 */
class AmendmentsCrossPipelineSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private val fixture = new ComposeStackFixture()

  private lazy val xa: Transactor[IO] = Transactor.fromDriverManager[IO](
    driver = "org.postgresql.Driver",
    url = fixture.alloydbJdbcUrl,
    user = fixture.alloydbUser,
    password = fixture.alloydbPassword,
    logHandler = None,
  )

  override def beforeAll(): Unit = {
    super.beforeAll()
    // The baseline stack (bills + members + votes) populates `bills` and `members` so the §S3 fan-out assertion
    // (one bill anchors two amendments) has a stable row to FK against, and S6 regression has the data it needs.
    fixture.start()
    // Seed the workflow_runs parent row so AmendmentsPipeline's WorkflowStateUpdater.recordStepStarted insert
    // satisfies the workflow_run_steps → workflow_runs FK. AmendmentsPipeline is the first pipeline in this stack
    // that actually writes workflow_run_steps; bills/members/votes pipelines either hardcode runId=0L (and skip the
    // updater) or use Optional updaters. The compose `command:` pins runId=1, and id=1 is safe here because no other
    // test in this suite writes to workflow_runs.
    seedWorkflowRun(id = 1L)
    fixture.startAmendments()
    // amendment-text-pipeline is a long-running subscriber. Block until either the SAMDT 100 text-version row
    // shows `fetched_at` non-NULL, or the timeout expires. 90s is generous — Ollama is stubbed (no real
    // inference), and chunking + DB write should be sub-second.
    waitForFetchedAt(timeoutMs = 90_000L, pollMs = 1_500L)
  }

  private def seedWorkflowRun(id: Long): Unit = {
    val idFr = doobie.Fragment.const(id.toString)
    val _ =
      sql"""INSERT INTO workflow_runs (id, workflow_name, status, triggered_by, started_at)
            VALUES ($idFr, 'amendments-e2e', 'running'::workflow_status_type, 'e2e-test', NOW())
            ON CONFLICT (id) DO NOTHING""".update.run.transact(xa).unsafeRunSync()
  }

  override def afterAll(): Unit = {
    try fixture.stop()
    catch { case _: Exception => () }
    super.afterAll()
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private def sqlLong(query: doobie.Fragment): Long =
    query.query[Long].unique.transact(xa).unsafeRunSync()

  private def sqlOptLong(query: doobie.Fragment): Option[Long] =
    query.query[Long].option.transact(xa).unsafeRunSync()

  private def sqlOptString(query: doobie.Fragment): Option[String] =
    query.query[String].option.transact(xa).unsafeRunSync()

  @tailrec
  private def waitForFetchedAt(timeoutMs: Long, pollMs: Long, elapsedMs: Long = 0L): Unit = {
    val fetched = sqlLong(
      sql"""SELECT COUNT(*) FROM amendment_text_versions
            WHERE fetched_at IS NOT NULL"""
    )
    if (fetched > 0L) {
      ()
    } else if (elapsedMs >= timeoutMs) {
      // Don't fail beforeAll — let the per-test assertions surface the diagnostic context instead.
      ()
    } else {
      Thread.sleep(pollMs)
      waitForFetchedAt(timeoutMs, pollMs, elapsedMs + pollMs)
    }
  }

  // ---------------------------------------------------------------------------
  // Scenario S1 — chain happy path. Bill → SAMDT 100 → SUAMDT 200 + SAMDT 101.
  // ---------------------------------------------------------------------------

  "amendments-pipeline" should "persist all three fixture amendments (S1)" taggedAs DockerRequired in {
    val count = sqlLong(sql"SELECT COUNT(*) FROM amendments WHERE congress = 117")
    // The fixture list has 3 amendments (SAMDT 100, SUAMDT 200, SAMDT 101). Recursion may insert intermediary
    // rows but for this fixture none are extra — every parent in the chain is also in the list. Equality is
    // tightest assertion; allow `>=` to tolerate any defensive duplicates that ON CONFLICT folds in.
    count shouldBe 3L
  }

  it should "persist SAMDT 100 with the chain anchor bill (S1)" taggedAs DockerRequired in {
    val billRow = sqlOptLong(
      sql"""SELECT b.id FROM amendments a
            JOIN bills b ON b.id = a.bill_id
            WHERE a.natural_key = '117-SAMDT-100'
              AND b.congress = 117 AND b.bill_type = 'hr' AND b.number = 3684"""
    )
    billRow shouldBe defined
  }

  it should "persist SUAMDT 200 with the resolved-ancestor bill (S1)" taggedAs DockerRequired in {
    // SUAMDT 200's direct parent is SAMDT 100 (an amendment, not a bill). Per §7.2 design — `bill_id` carries the
    // resolved-ancestor semantic — SUAMDT 200's `bill_id` must equal SAMDT 100's `bill_id`, NOT NULL.
    val billRow = sqlOptLong(
      sql"""SELECT b.id FROM amendments a
            JOIN bills b ON b.id = a.bill_id
            WHERE a.natural_key = '117-SUAMDT-200'
              AND b.congress = 117 AND b.bill_type = 'hr' AND b.number = 3684"""
    )
    billRow shouldBe defined
  }

  // ---------------------------------------------------------------------------
  // Scenario S3 — fan-out. Both SAMDTs share the same bill_id.
  // ---------------------------------------------------------------------------

  it should "anchor multiple amendments on the same bill (S3 fan-out)" taggedAs DockerRequired in {
    // Both SAMDT 100 and SAMDT 101 amend the same bill. Pull their bill_ids and confirm they match.
    val ids = sql"""SELECT bill_id FROM amendments
                    WHERE natural_key IN ('117-SAMDT-100', '117-SAMDT-101')
                    ORDER BY natural_key"""
      .query[Option[Long]]
      .to[List]
      .transact(xa)
      .unsafeRunSync()
    val _ = ids should have size 2
    ids.headOption shouldBe Some(ids(1))
  }

  // ---------------------------------------------------------------------------
  // Scenario S4 — text ingestion via checker → pipeline → fetched_at.
  // ---------------------------------------------------------------------------

  it should "have last_text_check_at set on every checker-eligible amendment (S4)" taggedAs DockerRequired in {
    // The checker's `findCandidatesForTextCheck` query filters with `amendment_type <> 'suamdt'` because
    // Congress.gov stores SUAMDT text on the parent amendment — sub-amendments are intentionally skipped to
    // avoid burning request budget on guaranteed-empty responses. So SUAMDT 200 keeps NULL last_text_check_at;
    // only the SAMDTs are expected to have it stamped.
    val unchecked = sqlLong(
      sql"""SELECT COUNT(*) FROM amendments
            WHERE congress = 117
              AND amendment_type <> 'suamdt'::amendment_type_enum
              AND last_text_check_at IS NULL"""
    )
    unchecked shouldBe 0L
  }

  it should "have fetched_at populated on SAMDT 100's text version (S4)" taggedAs DockerRequired in {
    // SAMDT 100 is the only fixture amendment with a non-empty textVersions[] from WireMock. The §7.5 checker
    // publishes one event; §7.6 consumes it, downloads via the rewriter, chunks + embeds, writes fetched_at.
    val fetched = sqlOptString(
      sql"""SELECT v.version_type FROM amendment_text_versions v
            JOIN amendments a ON a.id = v.amendment_id
            WHERE a.natural_key = '117-SAMDT-100'
              AND v.fetched_at IS NOT NULL"""
    )
    fetched shouldBe defined
  }

  it should "persist at least one chunk + embedding for SAMDT 100 (S4)" taggedAs DockerRequired in {
    val chunkCount = sqlLong(
      sql"""SELECT COUNT(*) FROM amendment_text_chunks c
            JOIN amendment_text_versions v ON v.id = c.version_id
            JOIN amendments a ON a.id = v.amendment_id
            WHERE a.natural_key = '117-SAMDT-100'
              AND c.embedding IS NOT NULL"""
    )
    chunkCount should be >= 1L
  }

  // ---------------------------------------------------------------------------
  // Scenario S6 — backwards-compat. The bills baseline still holds after the
  // amendments slice runs on top. (DockerComposeE2EStackSpec covers the full
  // regression — this is a smoke check that adding amendments didn't perturb
  // the underlying bill rows.)
  // ---------------------------------------------------------------------------

  it should "leave existing bills rows untouched after the amendments run (S6 smoke)" taggedAs DockerRequired in {
    val hr1 = sqlOptLong(
      sql"SELECT 1 FROM bills WHERE congress = 118 AND bill_type = 'hr' AND number = 1"
    )
    hr1 shouldBe defined
  }

  // AmendmentChainFixtures sample-chain self-check lives in
  // `AmendmentChainFixturesSpec` (same package), tagged for the default
  // `sbt test` (not DockerRequired) — runs every PR without the compose stack.

}
