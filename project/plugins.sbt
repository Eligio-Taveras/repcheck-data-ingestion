addSbtPlugin("org.typelevel"    % "sbt-tpolecat"      % "0.5.3")
addSbtPlugin("ch.epfl.scala"    % "sbt-scalafix"      % "0.12.0")
addSbtPlugin("org.scalameta"    % "sbt-scalafmt"       % "2.5.2")
addSbtPlugin("org.wartremover"  % "sbt-wartremover"    % "3.5.6")
addSbtPlugin("org.scoverage"    % "sbt-scoverage"      % "2.4.4")
addSbtPlugin("com.github.sbt"  % "sbt-dynver"         % "5.1.0")

resolvers += "GitHub Packages repcheck-sbt-plugins" at
  "https://maven.pkg.github.com/Eligio-Taveras/repcheck-sbt-plugins"
credentials += Credentials(
  "GitHub Package Registry",
  "maven.pkg.github.com",
  sys.env.getOrElse("GITHUB_USERNAME", sys.env.getOrElse("GITHUB_ACTOR", "")),
  sys.env.getOrElse("GITHUB_TOKEN", "")
)
addSbtPlugin("com.repcheck" % "sbt-exception-uniqueness" % "0.4.0")
