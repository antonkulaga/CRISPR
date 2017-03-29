import com.typesafe.sbt.SbtNativePackager.autoImport._
import org.scalajs.sbtplugin.ScalaJSPlugin.autoImport._
import sbt.Keys.{javaOptions, javacOptions, scalacOptions}
import sbtdocker._
import sbt._

//settings for all the projects
lazy val commonSettings = Seq(

	organization := "comp.bio.aging",

	scalaVersion :=  "2.11.8",

	version := "0.0.4",

	unmanagedClasspath in Compile ++= (unmanagedResources in Compile).value,

	updateOptions := updateOptions.value.withCachedResolution(true), //to speed up dependency resolution

	resolvers += sbt.Resolver.bintrayRepo("comp-bio-aging", "main"),

	resolvers += sbt.Resolver.bintrayRepo("denigma", "denigma-releases"),

	maintainer := "Anton Kulaga <antonkulaga@gmail.com>",

	packageDescription := """crispr-server""",

	bintrayRepository := "main",

	bintrayOrganization := Some("comp-bio-aging"),

	licenses += ("MPL-2.0", url("http://opensource.org/licenses/MPL-2.0")),

	isSnapshot := true,

	exportJars := true,

	scalacOptions ++= Seq( "-target:jvm-1.8", "-feature", "-language:_" ),

	parallelExecution in Test := false,

	javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint", "-J-Xss5M", "-encoding", "UTF-8"),

	initialCommands in (Test, console) := """ammonite.Main().run()"""
)

commonSettings

javaOptions ++= Seq("-Xms512M", "-Xmx3072M", "-XX:MaxPermSize=3072M", "-XX:+CMSClassUnloadingEnabled")

resourceDirectory in Test := baseDirectory { _ / "files" }.value

fork in run := true

parallelExecution in Test := false

packageSummary := "crispr-server"

libraryDependencies ++= Seq(

	"org.denigma" %% "akka-http-extensions" % "0.0.15",

	"com.typesafe.akka" %% "akka-stream" % "2.4.17",

	"net.sf.py4j" % "py4j" % "0.10.4",

	"com.lihaoyi" % "ammonite" % "0.8.2" % Test cross CrossVersion.full,

	"com.typesafe.akka" %% "akka-http-testkit" % "10.0.5" % Test,

  "org.scalatest" %% "scalatest" % "3.0.1" % Test
)

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
  .in(file("crispr"))
  .settings(commonSettings: _*)
  .settings(
    name := "crispr",
		libraryDependencies ++= Seq(
			"fr.hmil" %%% "roshttp" % "2.0.1",
			"com.lihaoyi" %%% "pprint" % "0.4.4",
			"org.scalatest" %%% "scalatest" % "3.0.1" % Test
		),
		libraryDependencies ++= Seq(
			"io.circe" %%% "circe-core",
			"io.circe" %%% "circe-generic",
			"io.circe" %%% "circe-parser"
		).map(_ % circeVersion)
	)
  .disablePlugins(RevolverPlugin)
  .jvmSettings(
    libraryDependencies ++= Seq(
			"org.apache.spark" %% "spark-sql" % "2.0.2",
		  "comp.bio.aging" %% "adam-playground" % "0.0.4",
			"com.github.pathikrit" %% "better-files" % "2.17.1",
			"com.lihaoyi" % "ammonite" % "0.8.2" % Test cross CrossVersion.full,
			"com.holdenkarau" %% "spark-testing-base" % "2.1.0_0.6.0" % Test
		)
  )
  .jsSettings(
    jsDependencies += RuntimeDOM % Test
  )

lazy val crisprJVM = crispr.jvm

lazy val crisprJS = crispr.js

