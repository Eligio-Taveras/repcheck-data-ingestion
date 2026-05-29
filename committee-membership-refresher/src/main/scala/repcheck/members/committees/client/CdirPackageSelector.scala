package repcheck.members.committees.client

/**
 * Pure mapping from a congress to its CDIR package, by matching the package's issue year to the congress's two years.
 */
object CdirPackageSelector {

  final case class CdirPackageRef(packageId: String, dateIssued: String)

  /** The two calendar years a congress spans (1st = 1789–1790). */
  def congressYears(congress: Int): (Int, Int) = {
    val start = 1789 + 2 * (congress - 1)
    (start, start + 1)
  }

  /** The latest CDIR package issued within the congress's two years, if any. */
  def selectForCongress(packages: List[CdirPackageRef], congress: Int): Option[String] = {
    val (y1, y2) = congressYears(congress)
    packages
      .filter(p => yearOf(p.dateIssued).exists(y => y == y1 || y == y2))
      .maxByOption(_.dateIssued)
      .map(_.packageId)
  }

  private def yearOf(dateIssued: String): Option[Int] =
    dateIssued.take(4).toIntOption

}
