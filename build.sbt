import com.typesafe.sbt.SbtNativePackager.autoImport._
import org.scalajs.sbtplugin.ScalaJSPlugin.autoImport._
import sbt.Keys.{javaOptions, javacOptions, scalacOptions}
import sbtdocker._
import sbt._

name := "crispr-server"

//settings for all the projects
lazy val commonSettings = Seq(

	organization := "comp.bio.aging",

	scalaVersion :=  "2.11.8",

	version := "0.0.1",

	unmanagedClasspath in Compile ++= (unmanagedResources in Compile).value,

	updateOptions := updateOptions.value.withCachedResolution(true), //to speed up dependency resolution

	resolvers += sbt.Resolver.bintrayRepo("comp-bio-aging", "main"),

	resolvers += sbt.Resolver.bintrayRepo("denigma", "denigma-releases"),

	resolvers += sbt.Resolver.bintrayRepo("shadaj", "denigma-releases"), // for scalapy

	maintainer := "Anton Kulaga <antonkulaga@gmail.com>",

	packageDescription := """crispr-server""",

	bintrayRepository := "main",

	bintrayOrganization := Some("comp-bio-aging"),

	licenses += ("MPL-2.0", url("http://opensource.org/licenses/MPL-2.0")),

	isSnapshot := true,

	exportJars := true,

	scalacOptions ++= Seq( "-target:jvm-1.8", "-feature", "-language:_" ),

	javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint", "-J-Xss5M", "-encoding", "UTF-8")
)

commonSettings

mainClass in Compile := Some("comp.bio.aging.crispr.server.WebSebrver")

resourceDirectory in Test := baseDirectory { _ / "files" }.value

fork in run := true

parallelExecution in Test := false

packageSummary := "crispr-server"

libraryDependencies ++= Seq(

	"org.denigma" %% "akka-http-extensions" % "0.0.15",

	"com.typesafe.akka" %% "akka-stream" % "2.4.16",

	"net.sf.py4j" % "py4j" % "0.10.4",

	"com.lihaoyi" % "ammonite" % "0.8.2" % Test cross CrossVersion.full,

	"com.typesafe.akka" %% "akka-http-testkit" % "10.0.1" % Test,

  "org.scalatest" %% "scalatest" % "3.0.1" % Test
)

initialCommands in (Test, console) := """ammonite.Main().run()"""

enablePlugins(sbtdocker.DockerPlugin, JavaServerAppPackaging)


dockerfile in docker := {
	val appDir = stage.value
	val targetDir = "/app"
	immutable.Dockerfile.empty
		.from("compbioaging/azimuth:latest")
		.run("pip install jep")
		.run("apt-get", "-y", "install", "openjdk-8-jre-headless")
		.cmdRaw("java -jar app.jar")
		.entryPoint(s"$targetDir/bin/${executableScriptName.value}")
		.copy(appDir, targetDir)
}

buildOptions in docker := sbtdocker.BuildOptions(
	cache = false,
	pullBaseImage = sbtdocker.BuildOptions.Pull.Always
)

lazy val circeVersion = "0.7.0"

lazy val crispr = crossProject
  .crossType(CrossType.Full)
  .in(file("client"))
  .settings(commonSettings: _*)
  .settings(
    name := "crispr",
		libraryDependencies ++= Seq(
			"fr.hmil" %%% "roshttp" % "2.0.1"
		),
		libraryDependencies ++= Seq(
			"io.circe" %%% "circe-core",
			"io.circe" %%% "circe-generic",
			"io.circe" %%% "circe-parser",
			"io.circe" %%% "circe-jawn"
		).map(_ % circeVersion)
	)
  .disablePlugins(RevolverPlugin)
  .jvmSettings(
		/*
    libraryDependencies ++= Seq(
     "org.apache.spark" %% "spark-core" % "2.0.2",
     "org.bdgenomics.adam" %% "adam-core-spark2" % "0.21.0"
		)
		*/
  )
  .jsSettings(
    jsDependencies += RuntimeDOM % Test
  )

lazy val crisprJVM = crispr.jvm

lazy val crisprJS = crispr.js

