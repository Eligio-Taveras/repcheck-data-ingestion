package repcheck.members.common.persistence

import java.time.Instant

import doobie.util.Write

import repcheck.shared.models.congress.common.{Party, UsState}
import repcheck.shared.models.congress.dos.member.MemberDO

object MemberWriteInstances {

  implicit val memberInsertWrite: Write[MemberDO] = {
    import doobie.postgres.implicits._
    import repcheck.shared.models.congress.common.DoobieEnumInstances._
    type Row = (
      String,          // naturalKey
      Option[String],  // firstName
      Option[String],  // lastName
      Option[String],  // directOrderName
      Option[String],  // invertedOrderName
      Option[String],  // honorificName
      Option[Int],     // birthYear
      Option[Party],   // currentParty
      Option[UsState], // state
      Option[Int],     // district
      Option[String],  // imageUrl
      Option[String],  // imageAttribution
      Option[String],  // officialUrl
      Option[Instant], // updateDate
    )
    Write[Row].contramap[MemberDO] { m =>
      (
        m.naturalKey,
        m.firstName,
        m.lastName,
        m.directOrderName,
        m.invertedOrderName,
        m.honorificName,
        m.birthYear,
        m.currentParty,
        m.state,
        m.district,
        m.imageUrl,
        m.imageAttribution,
        m.officialUrl,
        m.updateDate,
      )
    }
  }

}
