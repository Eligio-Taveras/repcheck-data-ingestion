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
      String,
      Option[String],
      Option[String],
      Option[String],
      Option[String],
      Option[String],
      Option[Int],
      Option[Party],
      Option[UsState],
      Option[Int],
      Option[String],
      Option[String],
      Option[String],
      Option[Instant],
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
