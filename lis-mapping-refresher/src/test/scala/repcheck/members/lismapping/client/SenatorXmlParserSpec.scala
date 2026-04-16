package repcheck.members.lismapping.client

import scala.xml.XML

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SenatorXmlParserSpec extends AnyFlatSpec with Matchers {

  private def xml(body: String) = XML.loadString(body)

  private val singleMemberXml =
    """<contact_information>
      |  <member>
      |    <lis_member_id>S300</lis_member_id>
      |    <bioguide_id>S000148</bioguide_id>
      |    <first_name>Jane</first_name>
      |    <last_name>Doe</last_name>
      |    <party>D</party>
      |    <state>NY</state>
      |    <class>1</class>
      |    <is_current>true</is_current>
      |    <service_dates>
      |      <service>
      |        <congress>118</congress>
      |        <start_date>2023-01-03</start_date>
      |        <end_date>2025-01-03</end_date>
      |      </service>
      |    </service_dates>
      |  </member>
      |</contact_information>""".stripMargin

  "parse" should "map a single <member> entry to one DTO with all fields populated" in {
    val result = SenatorXmlParser.parse(xml(singleMemberXml))
    val _      = result.size shouldBe 1
    val dto    = result.headOption.getOrElse(fail("expected one entry"))
    val _      = dto.lisId shouldBe "S300"
    val _      = dto.bioguideId shouldBe "S000148"
    val _      = dto.firstName shouldBe "Jane"
    val _      = dto.lastName shouldBe "Doe"
    val _      = dto.party shouldBe "D"
    val _      = dto.state shouldBe "NY"
    val _      = dto.senateClass shouldBe Some(1)
    val _      = dto.isCurrent shouldBe true
    val _      = dto.serviceDates.size shouldBe 1
    val period = dto.serviceDates.headOption.getOrElse(fail("expected one service period"))
    val _      = period.congress shouldBe Some(118)
    val _      = period.startDate shouldBe Some("2023-01-03")
    period.endDate shouldBe Some("2025-01-03")
  }

  it should "preserve order across multiple <member> entries" in {
    val body =
      """<contact_information>
        |  <member>
        |    <lis_member_id>S100</lis_member_id>
        |    <bioguide_id>B100</bioguide_id>
        |    <first_name>Alpha</first_name>
        |    <last_name>One</last_name>
        |    <party>D</party>
        |    <state>CA</state>
        |    <is_current>true</is_current>
        |  </member>
        |  <member>
        |    <lis_member_id>S200</lis_member_id>
        |    <bioguide_id>B200</bioguide_id>
        |    <first_name>Beta</first_name>
        |    <last_name>Two</last_name>
        |    <party>R</party>
        |    <state>TX</state>
        |    <is_current>false</is_current>
        |  </member>
        |  <member>
        |    <lis_member_id>S300</lis_member_id>
        |    <bioguide_id>B300</bioguide_id>
        |    <first_name>Gamma</first_name>
        |    <last_name>Three</last_name>
        |    <party>I</party>
        |    <state>VT</state>
        |    <is_current>true</is_current>
        |  </member>
        |</contact_information>""".stripMargin

    val result = SenatorXmlParser.parse(xml(body))
    val _      = result.size shouldBe 3
    result.map(_.lisId) shouldBe List("S100", "S200", "S300")
  }

  it should "leave senateClass as None when <class> is missing" in {
    val body =
      """<contact_information>
        |  <member>
        |    <lis_member_id>S400</lis_member_id>
        |    <bioguide_id>B400</bioguide_id>
        |    <first_name>NoClass</first_name>
        |    <last_name>Senator</last_name>
        |    <party>D</party>
        |    <state>WA</state>
        |    <is_current>true</is_current>
        |  </member>
        |</contact_information>""".stripMargin

    val dto = SenatorXmlParser.parse(xml(body)).headOption.getOrElse(fail("expected one entry"))
    dto.senateClass shouldBe None
  }

  it should "leave senateClass as None when <class> is non-numeric" in {
    val body =
      """<contact_information>
        |  <member>
        |    <lis_member_id>S401</lis_member_id>
        |    <bioguide_id>B401</bioguide_id>
        |    <first_name>Bad</first_name>
        |    <last_name>Class</last_name>
        |    <party>D</party>
        |    <state>WA</state>
        |    <class>abc</class>
        |    <is_current>true</is_current>
        |  </member>
        |</contact_information>""".stripMargin

    val dto = SenatorXmlParser.parse(xml(body)).headOption.getOrElse(fail("expected one entry"))
    dto.senateClass shouldBe None
  }

  it should "skip entries missing a required field (bioguide_id)" in {
    val body =
      """<contact_information>
        |  <member>
        |    <lis_member_id>S500</lis_member_id>
        |    <first_name>Ghost</first_name>
        |    <last_name>Senator</last_name>
        |    <party>D</party>
        |    <state>NY</state>
        |  </member>
        |  <member>
        |    <lis_member_id>S501</lis_member_id>
        |    <bioguide_id>B501</bioguide_id>
        |    <first_name>Valid</first_name>
        |    <last_name>Senator</last_name>
        |    <party>D</party>
        |    <state>NY</state>
        |    <is_current>true</is_current>
        |  </member>
        |</contact_information>""".stripMargin

    val result = SenatorXmlParser.parse(xml(body))
    val _      = result.size shouldBe 1
    result.map(_.lisId) shouldBe List("S501")
  }

  it should "return an empty list when <contact_information> has no <member> entries" in {
    val body = "<contact_information></contact_information>"
    SenatorXmlParser.parse(xml(body)) shouldBe empty
  }

  it should "parse multiple <service> periods per member" in {
    val body =
      """<contact_information>
        |  <member>
        |    <lis_member_id>S600</lis_member_id>
        |    <bioguide_id>B600</bioguide_id>
        |    <first_name>Many</first_name>
        |    <last_name>Terms</last_name>
        |    <party>D</party>
        |    <state>NY</state>
        |    <is_current>true</is_current>
        |    <service_dates>
        |      <service>
        |        <congress>116</congress>
        |        <start_date>2019-01-03</start_date>
        |        <end_date>2021-01-03</end_date>
        |      </service>
        |      <service>
        |        <congress>117</congress>
        |        <start_date>2021-01-03</start_date>
        |        <end_date>2023-01-03</end_date>
        |      </service>
        |      <service>
        |        <congress>118</congress>
        |        <start_date>2023-01-03</start_date>
        |        <end_date>2025-01-03</end_date>
        |      </service>
        |    </service_dates>
        |  </member>
        |</contact_information>""".stripMargin

    val dto = SenatorXmlParser.parse(xml(body)).headOption.getOrElse(fail("expected one entry"))
    val _   = dto.serviceDates.size shouldBe 3
    dto.serviceDates.flatMap(_.congress) shouldBe List(116, 117, 118)
  }

  it should "treat is_current as false when missing or any non-truthy value" in {
    val body =
      """<contact_information>
        |  <member>
        |    <lis_member_id>S700</lis_member_id>
        |    <bioguide_id>B700</bioguide_id>
        |    <first_name>NoFlag</first_name>
        |    <last_name>Senator</last_name>
        |    <party>D</party>
        |    <state>NY</state>
        |  </member>
        |  <member>
        |    <lis_member_id>S701</lis_member_id>
        |    <bioguide_id>B701</bioguide_id>
        |    <first_name>Former</first_name>
        |    <last_name>Senator</last_name>
        |    <party>D</party>
        |    <state>NY</state>
        |    <is_current>no</is_current>
        |  </member>
        |</contact_information>""".stripMargin

    val result = SenatorXmlParser.parse(xml(body))
    val _      = result.size shouldBe 2
    result.map(_.isCurrent) shouldBe List(false, false)
  }

  it should "accept variant truthy values for is_current (yes, 1)" in {
    val body =
      """<contact_information>
        |  <member>
        |    <lis_member_id>S800</lis_member_id>
        |    <bioguide_id>B800</bioguide_id>
        |    <first_name>Yes</first_name>
        |    <last_name>Variant</last_name>
        |    <party>D</party>
        |    <state>NY</state>
        |    <is_current>yes</is_current>
        |  </member>
        |  <member>
        |    <lis_member_id>S801</lis_member_id>
        |    <bioguide_id>B801</bioguide_id>
        |    <first_name>One</first_name>
        |    <last_name>Variant</last_name>
        |    <party>D</party>
        |    <state>NY</state>
        |    <is_current>1</is_current>
        |  </member>
        |</contact_information>""".stripMargin

    val result = SenatorXmlParser.parse(xml(body))
    val _      = result.size shouldBe 2
    result.map(_.isCurrent) shouldBe List(true, true)
  }

  it should "return an empty list when the document has no <member> elements anywhere" in {
    val body = "<root><unrelated>content</unrelated></root>"
    SenatorXmlParser.parse(xml(body)) shouldBe empty
  }

  it should "skip a <member> with an empty bioguide_id element" in {
    val body =
      """<contact_information>
        |  <member>
        |    <lis_member_id>S900</lis_member_id>
        |    <bioguide_id></bioguide_id>
        |    <first_name>Blank</first_name>
        |    <last_name>Bio</last_name>
        |    <party>D</party>
        |    <state>NY</state>
        |  </member>
        |</contact_information>""".stripMargin

    SenatorXmlParser.parse(xml(body)) shouldBe empty
  }

}
