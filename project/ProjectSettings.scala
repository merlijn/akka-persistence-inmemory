import sbt._
import sbt.Keys._
import sbtprotoc.ProtocPlugin.autoImport.PB

object ProjectSettings extends AutoPlugin {

  final val AkkaVersion = "2.4.20"
  final val ScalazVersion = "7.2.17"
  final val ScalaTestVersion = "3.0.4"
  final val LogbackVersion = "1.2.3"

  override def requires = plugins.JvmPlugin
  override def trigger = allRequirements

  val scalapbVersion = scalapb.compiler.Version.scalapbVersion

  lazy val testSettings = Seq(
    fork in Test := true,
    logBuffered in Test := false,
    parallelExecution in Test := false,
    // show full stack traces and test case durations
    testOptions in Test += Tests.Argument("-oDF")
  )

  lazy val resolverSettings = Seq(
    resolvers += Resolver.sonatypeRepo("public"),
    resolvers += Resolver.typesafeRepo("releases"),
    resolvers += Resolver.jcenterRepo
  )

  lazy val compilerSettings = Seq(
    scalacOptions ++= Seq(
      "-encoding",
      "UTF-8",
      "-deprecation",
      "-feature",
      "-unchecked",
      "-Xlog-reflective-calls",
      "-language:higherKinds",
      "-language:implicitConversions",
      "-Ypartial-unification",
      "-target:jvm-1.8",
      "-Ydelambdafy:method"
    )
  )

  lazy val scalaPBSettings = Seq(PB.targets in Compile := Seq(scalapb.gen() -> (sourceManaged in Compile).value))

  lazy val librarySettings = Seq(
    libraryDependencies ++= Seq("com.typesafe.akka" %% "akka-actor" % AkkaVersion,
      "com.typesafe.akka" %% "akka-persistence" % AkkaVersion,
      "com.typesafe.akka" %% "akka-persistence-query-experimental" % AkkaVersion,
      "com.typesafe.akka" %% "akka-stream" % AkkaVersion,
      "com.google.protobuf" % "protobuf-java" % "3.5.1",
      "ch.qos.logback" % "logback-classic" % LogbackVersion % Test,
      "com.typesafe.akka" %% "akka-slf4j" % AkkaVersion % Test,
      "com.github.pathikrit" %% "better-files" % "3.6.0",
      "com.typesafe.akka" %% "akka-persistence-tck" % AkkaVersion % Test,
      "com.typesafe.akka" %% "akka-stream-testkit" % AkkaVersion % Test,
      "com.typesafe.akka" %% "akka-testkit" % AkkaVersion % Test,
      "com.thesamet.scalapb" %% "scalapb-runtime"  % scalapbVersion % "protobuf",
      "org.scalatest" %% "scalatest" % ScalaTestVersion % Test)
  )

  override def projectSettings = Seq(
    name := "akka-persistence-inmemory",
    organization := "com.github.dnvriend",
    organizationName := "Dennis Vriend",
    description := "A plugin for storing events in an event journal akka-persistence-inmemory",
    startYear := Some(2014),

    scalaVersion := "2.12.6",
    crossScalaVersions := Seq("2.11.11", "2.12.6"),
    crossVersion := CrossVersion.binary,

    licenses := Seq(("Apache-2.0", new URL("https://www.apache.org/licenses/LICENSE-2.0.txt")))

  ) ++ compilerSettings ++ resolverSettings ++ librarySettings ++ testSettings ++ scalaPBSettings
}
