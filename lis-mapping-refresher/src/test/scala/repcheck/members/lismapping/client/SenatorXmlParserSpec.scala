package repcheck.members.lismapping.client

import scala.xml.XML

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/**
 * Tests for [[SenatorXmlParser]] using the real `senator-lookup.xml` schema from senate.gov:
 *   - Root element: `<senators>`
 *   - Entry element: `<senator>`
 *   - LIS ID: `<lisid>` (not `<lis_member_id>`)
 *   - Bioguide: `<bioguide>` (not `<bioguide_id>`)
 *   - Names: nested under `<full_name><first_name>` / `<full_name><last_name>`
 *   - Senate class: `class` attribute on `<service_date class="N">`
 *   - Currently serving: any `<service_date>` with `<end_date year="">` (empty year)
 *   - Congress terms: `<congresses><congress>N</congress>` list
 */
class SenatorXmlParserSpec extends AnyFlatSpec with Matchers {

  private def xml(body: String) = XML.loadString(body)

  // Minimal helper: builds a senator entry in the real senator-lookup.xml format.
  // lisId and bioguide are required. The senator is currently serving if endYear is empty ("").
  private def senator(
    lisId: String,
    bioguide: String,
    firstName: String = "Jane",
    lastName: String = "Doe",
    party: String = "D",
    state: String = "NY",
    senateClass: Int = 1,
    isCurrent: Boolean,
    congresses: Seq[Int],
  ): String = {
    val congressXml = congresses.map(c => s"<congress>${c.toString}</congress>").mkString
    val endDateAttr =
      if (isCurrent) """day="" month="" year=""""" else """congress="118" day="03" month="01" year="2025""""
    s"""<senator>
       |  <full_name>
       |    <first_name>$firstName</first_name>
       |    <last_name>$lastName</last_name>
       |  </full_name>
       |  <party>$party</party>
       |  <state>$state</state>
       |  <bioguide>$bioguide</bioguide>
       |  <lisid>$lisId</lisid>
       |  <congresses>$congressXml</congresses>
       |  <service_dates>
       |    <service_date class="${senateClass.toString}">
       |      <begin_date congress="${congresses.headOption.getOrElse(118).toString}" day="03" month="01" year="2023"/>
       |      <end_date $endDateAttr/>
       |    </service_date>
       |  </service_dates>
       |</senator>""".stripMargin
  }

  private def document(senators: Seq[String]): String =
    s"<senators>${senators.mkString}</senators>"

  "parse" should "map a single <senator> entry to one DTO with all fields populated" in {
    val body   = document(Seq(senator("S300", "S000148", isCurrent = true, congresses = Seq(118))))
    val result = SenatorXmlParser.parse(xml(body))
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
    // Congress terms from <congresses> carry only the congress number; start/end dates are not populated
    val _ = period.startDate shouldBe None
    period.endDate shouldBe None
  }

  it should "preserve order across multiple <senator> entries" in {
    val body = document(
      Seq(
        senator("S100", "B100", isCurrent = true, congresses = Seq(118)),
        senator("S200", "B200", isCurrent = false, congresses = Seq(118)),
        senator("S300", "B300", isCurrent = true, congresses = Seq(118)),
      )
    )

    val result = SenatorXmlParser.parse(xml(body))
    val _      = result.size shouldBe 3
    result.map(_.lisId) shouldBe List("S100", "S200", "S300")
  }

  it should "leave senateClass as None when the service_date class attribute is absent" in {
    val body =
      """<senators>
        |  <senator>
        |    <full_name><first_name>NoClass</first_name><last_name>Senator</last_name></full_name>
        |    <party>D</party>
        |    <state>WA</state>
        |    <bioguide>B400</bioguide>
        |    <lisid>S400</lisid>
        |    <congresses><congress>118</congress></congresses>
        |    <service_dates>
        |      <service_date>
        |        <end_date day="" month="" year=""/>
        |      </service_date>
        |    </service_dates>
        |  </senator>
        |</senators>""".stripMargin

    val dto = SenatorXmlParser.parse(xml(body)).headOption.getOrElse(fail("expected one entry"))
    dto.senateClass shouldBe None
  }

  it should "leave senateClass as None when the class attribute is non-numeric" in {
    val body =
      """<senators>
        |  <senator>
        |    <full_name><first_name>Bad</first_name><last_name>Class</last_name></full_name>
        |    <party>D</party>
        |    <state>WA</state>
        |    <bioguide>B401</bioguide>
        |    <lisid>S401</lisid>
        |    <congresses><congress>118</congress></congresses>
        |    <service_dates>
        |      <service_date class="abc">
        |        <end_date day="" month="" year=""/>
        |      </service_date>
        |    </service_dates>
        |  </senator>
        |</senators>""".stripMargin

    val dto = SenatorXmlParser.parse(xml(body)).headOption.getOrElse(fail("expected one entry"))
    dto.senateClass shouldBe None
  }

  it should "skip entries missing a required field (bioguide)" in {
    val body =
      s"""<senators>
         |  <senator>
         |    <full_name><first_name>Ghost</first_name><last_name>Senator</last_name></full_name>
         |    <party>D</party><state>NY</state>
         |    <lisid>S500</lisid>
         |    <congresses><congress>118</congress></congresses>
         |  </senator>
         |  ${senator("S501", "B501", isCurrent = true, congresses = Seq(118))}
         |</senators>""".stripMargin

    val result = SenatorXmlParser.parse(xml(body))
    val _      = result.size shouldBe 1
    result.map(_.lisId) shouldBe List("S501")
  }

  it should "return an empty list when <senators> has no <senator> entries" in {
    SenatorXmlParser.parse(xml("<senators></senators>")) shouldBe empty
  }

  it should "map congress terms from <congresses> into serviceDates" in {
    val body =
      """<senators>
        |  <senator>
        |    <full_name><first_name>Many</first_name><last_name>Terms</last_name></full_name>
        |    <party>D</party><state>NY</state>
        |    <bioguide>B600</bioguide>
        |    <lisid>S600</lisid>
        |    <congresses>
        |      <congress>116</congress>
        |      <congress>117</congress>
        |      <congress>118</congress>
        |    </congresses>
        |    <service_dates>
        |      <service_date class="1">
        |        <begin_date congress="116" day="03" month="01" year="2019"/>
        |        <end_date day="" month="" year=""/>
        |      </service_date>
        |    </service_dates>
        |  </senator>
        |</senators>""".stripMargin

    val dto = SenatorXmlParser.parse(xml(body)).headOption.getOrElse(fail("expected one entry"))
    val _   = dto.serviceDates.size shouldBe 3
    dto.serviceDates.flatMap(_.congress) shouldBe List(116, 117, 118)
  }

  it should "set isCurrent=false when all service_date end_dates have non-empty years" in {
    val body =
      """<senators>
        |  <senator>
        |    <full_name><first_name>Former</first_name><last_name>Senator</last_name></full_name>
        |    <party>D</party><state>NY</state>
        |    <bioguide>B700</bioguide>
        |    <lisid>S700</lisid>
        |    <congresses><congress>118</congress></congresses>
        |    <service_dates>
        |      <service_date class="1">
        |        <begin_date congress="118" day="03" month="01" year="2023"/>
        |        <end_date congress="118" day="03" month="01" year="2025"/>
        |      </service_date>
        |    </service_dates>
        |  </senator>
        |</senators>""".stripMargin

    val dto = SenatorXmlParser.parse(xml(body)).headOption.getOrElse(fail("expected one entry"))
    dto.isCurrent shouldBe false
  }

  it should "set isCurrent=false when there are no service_dates at all" in {
    val body =
      """<senators>
        |  <senator>
        |    <full_name><first_name>NoDate</first_name><last_name>Senator</last_name></full_name>
        |    <party>D</party><state>NY</state>
        |    <bioguide>B701</bioguide>
        |    <lisid>S701</lisid>
        |    <congresses><congress>118</congress></congresses>
        |  </senator>
        |</senators>""".stripMargin

    val dto = SenatorXmlParser.parse(xml(body)).headOption.getOrElse(fail("expected one entry"))
    dto.isCurrent shouldBe false
  }

  it should "set isCurrent=true when any service_date has an empty end_date year" in {
    val body =
      """<senators>
        |  <senator>
        |    <full_name><first_name>Current</first_name><last_name>Senator</last_name></full_name>
        |    <party>D</party><state>NY</state>
        |    <bioguide>B800</bioguide>
        |    <lisid>S800</lisid>
        |    <congresses><congress>118</congress></congresses>
        |    <service_dates>
        |      <service_date class="1">
        |        <begin_date congress="118" day="03" month="01" year="2023"/>
        |        <end_date day="" month="" year=""/>
        |      </service_date>
        |    </service_dates>
        |  </senator>
        |</senators>""".stripMargin

    val dto = SenatorXmlParser.parse(xml(body)).headOption.getOrElse(fail("expected one entry"))
    dto.isCurrent shouldBe true
  }

  it should "return an empty list when the document has no <senator> elements" in {
    SenatorXmlParser.parse(xml("<root><unrelated>content</unrelated></root>")) shouldBe empty
  }

  it should "skip a <senator> with an empty <bioguide> element" in {
    val body =
      """<senators>
        |  <senator>
        |    <full_name><first_name>Blank</first_name><last_name>Bio</last_name></full_name>
        |    <party>D</party><state>NY</state>
        |    <bioguide></bioguide>
        |    <lisid>S900</lisid>
        |    <congresses><congress>118</congress></congresses>
        |  </senator>
        |</senators>""".stripMargin

    SenatorXmlParser.parse(xml(body)) shouldBe empty
  }

}
