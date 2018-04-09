// repositories
resolvers ++= Seq(
  Resolver.url("SBT assembly", url("http://dl.bintray.com/eed3si9n/sbt-plugins/"))(Resolver.ivyStylePatterns)
)

addSbtPlugin("org.scalariform" % "sbt-scalariform" % "1.8.2")

addSbtPlugin("org.scalaxb" % "sbt-scalaxb" % "1.5.2")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.6")