package repcheck.ingestion.votes.errors

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Message-format spec for the error classes added by the processor PR. Each class is expected to:
 *   - Include a unique prefix that operators can log-search on.
 *   - Include the identifying natural key or bioguide in the message.
 *   - Expose the wrapped `cause` via `getCause` when supplied.
 */
class VoteProcessorErrorsSpec extends AnyFlatSpec with Matchers {

  "VoteProcessingFailed" should "render 'Failed to process vote <naturalKey>: <detail>' without a cause" in {
    val err = VoteProcessingFailed(naturalKey = "119-House-1-42", detail = "conversion failed")
    val _   = err.getMessage shouldBe "Failed to process vote 119-House-1-42: conversion failed"
    Option(err.getCause) shouldBe None
  }

  it should "expose the wrapped cause when one is supplied" in {
    val cause = new RuntimeException("boom")
    val err   = VoteProcessingFailed("119-House-1-42", "publish failed", Some(cause))
    err.getCause shouldBe cause
  }

  "VoteConversionFailed" should "render 'Conversion failed for vote <naturalKey>: <detail>'" in {
    val err = VoteConversionFailed("119-Senate-1-17", "congress must be > 0, got: 0")
    err.getMessage shouldBe "Conversion failed for vote 119-Senate-1-17: congress must be > 0, got: 0"
  }

  "MemberResolutionFailed" should "render 'Failed to resolve member <bioguide>: <detail>'" in {
    val err = MemberResolutionFailed(bioguideId = "A000055", detail = "placeholder row disappeared")
    err.getMessage shouldBe "Failed to resolve member A000055: placeholder row disappeared"
  }

  "BillResolutionFailed" should "render 'Failed to resolve bill <naturalKey>: <detail>'" in {
    val err = BillResolutionFailed(billNaturalKey = "119-HR-1234", detail = "placeholder row disappeared")
    err.getMessage shouldBe "Failed to resolve bill 119-HR-1234: placeholder row disappeared"
  }

}
