import sbt._
import Keys._

object SpnlBuild extends Build {
  var commonResolvers = Seq(
    // local snapshot support
    ScalaToolsSnapshots,

    "Wajam" at "http://ci1.cx.wajam/",
    "Maven.org" at "http://repo1.maven.org/maven2",
    "Sun Maven2 Repo" at "http://download.java.net/maven/2",
    "Scala-Tools" at "http://scala-tools.org/repo-releases/",
    "Sun GF Maven2 Repo" at "http://download.java.net/maven/glassfish",
    "Oracle Maven2 Repo" at "http://download.oracle.com/maven",
    "Sonatype" at "http://oss.sonatype.org/content/repositories/release",
    "Cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/",
    "Twitter" at "http://maven.twttr.com/"
  )

  var commonDeps = Seq(
    "com.wajam" %% "commons-core" % "0.1-SNAPSHOT",
    "com.wajam" %% "nrv-core" % "0.1-SNAPSHOT",
    "com.wajam" %% "nrv-extension" % "0.1-SNAPSHOT",
    "com.wajam" %% "nrv-zookeeper" % "0.1-SNAPSHOT",
    "net.liftweb" %% "lift-json" % "2.5-RC4",
    "org.scalatest" %% "scalatest" % "2.0" % "test,it",
    "junit" % "junit" % "4.10" % "test,it",
    "org.mockito" % "mockito-core" % "1.9.0" % "test,it"
  )

  val defaultSettings = Defaults.defaultSettings ++ Defaults.itSettings ++ Seq(
    libraryDependencies ++= commonDeps,
    resolvers ++= commonResolvers,
    retrieveManaged := true,
    publishMavenStyle := true,
    organization := "com.wajam",
    version := "0.1-SNAPSHOT",
    scalaVersion := "2.10.2",
    scalacOptions ++= Seq("-deprecation", "-unchecked", "-feature")
  )

  lazy val root = Project(
    id = "spnl",
    base = file("."),
    settings = defaultSettings ++ Seq(
      // some other
    )
  ) configs (IntegrationTest) aggregate (core)

  lazy val core = Project(
    id = "spnl-core",
    base = file("spnl-core"),
    settings = defaultSettings ++ Seq(
      // some other
    )
  ) configs (IntegrationTest)
}

