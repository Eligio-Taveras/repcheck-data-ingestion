package repcheck.members.common.errors

/**
 * Raised by `MemberRepository.upsertPlaceholder` when the supplied bioguide id does not match the canonical
 * Congress.gov format `[A-Z]\d{6}` (e.g. `A000370`). Validating the format before issuing SQL keeps malformed values
 * out of `members.natural_key` and surfaces the bug at the call site instead of much later when
 * `MemberRepository.findByBioguideId` silently returns `None`.
 */
final case class InvalidBioguideId(value: String)
    extends Exception(
      s"Invalid bioguide id '$value': expected format [A-Z]\\d{6} (e.g. A000370)"
    )
