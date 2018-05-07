///
 // Copyright (C) 2013 Klout Inc. <http://www.klout.com>
 ///

import scalariform.formatter.preferences._
import sbtassembly.{MergeStrategy, PathList}

name := "scoozie"

organization := "com.klout"

version := "0.6.0-SNAPSHOT"

scalaVersion := "2.11.12"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) =>
    xs map {_.toLowerCase} match {
      case "services" :: xs => MergeStrategy.filterDistinctLines
      case "spring.schemas" :: Nil | "spring.handlers" :: Nil => MergeStrategy.filterDistinctLines
      case _ => MergeStrategy.discard
    }
  case _                       => MergeStrategy.first
}

val hdpV = "2.6.3.0-235"

val oozieV = s"4.2.0.$hdpV"
val hadoopV = s"2.7.3.$hdpV"

libraryDependencies ++= Seq(
    "org.specs2"             %% "specs2-core"              % "2.4.11" % Test,
    "org.scala-lang.modules" %% "scala-xml"                % "1.1.0",
    "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.0",
    "com.google.guava"       % "guava"                     % "18.0",
    "org.apache.oozie"       % "oozie-client"              % oozieV % "provided",
    "org.apache.oozie"       % "oozie-core"                % oozieV % "provided",
    "org.apache.hadoop"      % "hadoop-common"             % hadoopV % "provided"
).map(_ exclude("javax.jms", "jms"))


resolvers ++= Seq(
    "snapshots"   at "http://oss.sonatype.org/content/repositories/snapshots",
    "releases"    at "http://oss.sonatype.org/content/repositories/releases",
    "hortonworks" at "http://repo.hortonworks.com/content/repositories/releases",
    "spring"      at "http://repo.spring.io/plugins-release"   //    "org.pentaho" % "pentaho-aggdesigner-algorithm" % "5.1.5-jhyde"
)

scalariformAutoformat := true

scalariformPreferences := scalariformPreferences.value
  .setPreference(AlignParameters, true)
  .setPreference(IndentSpaces, 4)
  .setPreference(AlignSingleLineCaseStatements, true)
  .setPreference(PreserveSpaceBeforeArguments, true)

enablePlugins(ScalaxbPlugin)
scalaxbPackageName in (Compile, scalaxb) := "oozie"
scalaxbProtocolPackageName in (Compile, scalaxb) := Some("protocol")
scalaxbPackageNames in (Compile, scalaxb) := Map(
  uri("uri:oozie:workflow:0.5") -> "oozie.workflow",
  uri("uri:oozie:hive-action:0.5") -> "oozie.hive"
)

scalacOptions ++= Seq(
    "-unchecked",
    "-feature",
    "-language:existentials",
    "-language:postfixOps",
    "-language:implicitConversions"
)

publishTo := Some("kloutLibraryReleases" at "http://maven-repo:8081/artifactory/libs-release-local")

credentials := Credentials(Path.userHome / ".ivy2" / ".credentials") :: Nil
