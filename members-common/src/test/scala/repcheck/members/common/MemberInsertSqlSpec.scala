package repcheck.members.common

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class MemberInsertSqlSpec extends AnyFlatSpec with Matchers {

  "MemberInsertSql.value" should "be non-empty" in {
    MemberInsertSql.value should not be empty
  }

  it should "contain INSERT INTO members" in {
    MemberInsertSql.value should include("INSERT INTO members")
  }

  it should "contain ON CONFLICT clause" in {
    MemberInsertSql.value should include("ON CONFLICT (natural_key) DO NOTHING")
  }

  it should "contain all expected column names" in {
    val sql = MemberInsertSql.value
    val _   = sql should include("natural_key")
    val _   = sql should include("first_name")
    val _   = sql should include("last_name")
    val _   = sql should include("direct_order_name")
    val _   = sql should include("inverted_order_name")
    val _   = sql should include("honorific_name")
    val _   = sql should include("birth_year")
    val _   = sql should include("current_party")
    val _   = sql should include("state")
    val _   = sql should include("district")
    val _   = sql should include("image_url")
    val _   = sql should include("image_attribution")
    val _   = sql should include("official_url")
    sql should include("update_date")
  }

  it should "contain 14 parameter placeholders" in {
    val placeholderCount = MemberInsertSql.value.count(_ == '?')
    placeholderCount shouldBe 14
  }

}
