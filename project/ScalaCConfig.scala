import org.typelevel.scalacoptions.ScalacOption

object ScalaCConfig {
  val scalaCOptions: Set[ScalacOption] = Set(
    ScalacOption("-Xfatal-warnings", _ => true),
    ScalacOption("-encoding", _ => true),
    ScalacOption("utf-8", _ => true),
    ScalacOption("-explaintypes", _ => true),
    ScalacOption("-feature", _ => true),
    ScalacOption("-language:existentials", _ => true),
    ScalacOption("-language:experimental.macros", _ => true),
    ScalacOption("-language:higherKinds", _ => true),
    ScalacOption("-language:implicitConversions", _ => true),
    ScalacOption("-unchecked", _ => true),
    ScalacOption("-Xlint:adapted-args", _ => true),
    ScalacOption("-Xlint:constant", _ => true),
    ScalacOption("-Xlint:delayedinit-select", _ => true),
    ScalacOption("-Xlint:deprecation", _ => true),
    ScalacOption("-Xlint:doc-detached", _ => true),
    ScalacOption("-Xlint:inaccessible", _ => true),
    ScalacOption("-Xlint:infer-any", _ => true),
    ScalacOption("-Xlint:missing-interpolator", _ => true),
    ScalacOption("-Xlint:nullary-override", _ => true),
    ScalacOption("-Xlint:nullary-unit", _ => true),
    ScalacOption("-Xlint:option-implicit", _ => true),
    ScalacOption("-Xlint:package-object-classes", _ => true),
    ScalacOption("-Xlint:poly-implicit-overload", _ => true),
    ScalacOption("-Xlint:private-shadow", _ => true),
    ScalacOption("-Xlint:stars-align", _ => true),
    ScalacOption("-Xlint:type-parameter-shadow", _ => true),
    ScalacOption("-Wdead-code", _ => true),
    ScalacOption("-Wextra-implicit", _ => true),
    ScalacOption("-Wnumeric-widen", _ => true),
    ScalacOption("-Wunused:implicits", _ => true),
    ScalacOption("-Wunused:imports", _ => true),
    ScalacOption("-Wunused:locals", _ => true),
    ScalacOption("-Wunused:params", _ => true),
    ScalacOption("-Wunused:patvars", _ => true),
    ScalacOption("-Wunused:privates", _ => true),
    ScalacOption("-Wvalue-discard", _ => true)
  )

  val scalaCOption2_12: Set[ScalacOption] = Set(
    ScalacOption("-XCheckInit", _ => true),
    ScalacOption("-Ymacro-annotations", _ => true)
  )
}
