package repcheck.ingestion.members.profile.e2e

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import repcheck.members.common.testing.E2ETest

/**
 * End-to-end test stubs for the Member Profile Pipeline.
 *
 * These stubs define the full E2E test surface but leave implementations pending until
 * [[repcheck.ingestion.members.profile.app.MemberProfilePipelineApp]] (Phase 5) is complete.
 *
 * When implemented, each test will wire:
 *   - WireMock for Congress.gov API (`/v3/member/congress/{congress}`, `/v3/member/{bioguideId}`)
 *   - AlloyDB Omni (Docker) for persistence verification
 *   - Pub/Sub emulator for `member-updated` event verification
 *
 * Run with: `sbt "testOnly -- -n com.repcheck.tags.E2ETest"`
 */
class MemberProfilePipelineE2ESpec extends AnyFlatSpec with Matchers {

  "MemberProfilePipeline E2E" should
    "fetch all members for the configured congress from Congress.gov and persist them to AlloyDB" taggedAs E2ETest ignore {
      pending
    }

  it should "emit a member-updated event on the Pub/Sub topic for every House member processed" taggedAs E2ETest ignore {
    pending
  }

  it should
    "emit a member-updated event for a Senate member that already has a LIS mapping in the DB" taggedAs E2ETest ignore {
      pending
    }

  it should
    "skip emitting a member-updated event for a Senate member with no LIS mapping in the DB" taggedAs E2ETest ignore {
      pending
    }

  it should "write a member_history row before overwriting an existing member with updated data" taggedAs E2ETest ignore {
    pending
  }

  it should "fill a placeholder member created by the bills pipeline with full profile data" taggedAs E2ETest ignore {
    pending
  }

  it should
    "process members across multiple congresses when configured with congresses = [117, 118]" taggedAs E2ETest ignore {
      pending
    }

  it should "exit with ExitCode.Success when all members are processed without failures" taggedAs E2ETest ignore {
    pending
  }

  it should "exit with ExitCode.Error when one or more member fetches fail after retries" taggedAs E2ETest ignore {
    pending
  }

}
