import com.typesafe.sbt.SbtNativePackager.autoImport._
import org.scalajs.sbtplugin.ScalaJSPlugin.autoImport._
import sbt.Keys.{javaOptions, javacOptions, scalacOptions}
import sbt._

//settings for all the projects
lazy val commonSettings = Seq(

	organization := "comp.bio.aging",

	scalaVersion :=  "2.11.12",

	version := "0.0.7",

  coursierMaxIterations := 200,

	unmanagedClasspath in Compile ++= (unmanagedResources in Compile).value,

	updateOptions := updateOptions.value.withCachedResolution(true), //to speed up dependency resolution

	resolvers += Resolver.mavenLocal,

	resolvers += Resolver.sonatypeRepo("releases"),

	resolvers += sbt.Resolver.bintrayRepo("comp-bio-aging", "main"),

	resolvers += sbt.Resolver.bintrayRepo("denigma", "denigma-releases"),

	bintrayRepository := "main",

	bintrayOrganization := Some("comp-bio-aging"),

	licenses += ("MPL-2.0", url("http://opensource.org/licenses/MPL-2.0")),

	isSnapshot := true,

	exportJars := true,

	scalacOptions ++= Seq( "-target:jvm-1.8", "-feature", "-language:_" ),

	parallelExecution in Test := false,

	javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-Xlint", "-J-Xss5M", "-encoding", "UTF-8")

)

commonSettings

javaOptions ++= Seq("-Xms512M", "-Xmx3072M", "-XX:MaxPermSize=3072M", "-XX:+CMSClassUnloadingEnabled")

resourceDirectory in Test := baseDirectory { _ / "files" }.value

fork in run := true

parallelExecution in Test := false

enablePlugins(DockerPlugin, JavaServerAppPackaging)

lazy val framelessVersion = "0.4.0"

lazy val crispr = crossProject
  .crossType(CrossType.Full)
  .in(file("crispr"))
  .settings(commonSettings: _*)
  .settings(
    name := "crispr",
		libraryDependencies ++= Seq(
      "com.lihaoyi" %%% "pprint" % "0.5.3",
			"com.pepegar" %%% "hammock-circe" % "0.8.1",
      "org.scalatest" %%% "scalatest" % "3.0.4" % Test,
      "fr.hmil" %%% "roshttp" % "2.1.0" % Test
    )
	)
  .jvmSettings(
    libraryDependencies ++= Seq(
			"comp.bio.aging" %% "adam-playground" % "0.0.8",
			"org.hammerlab" %% "magic-rdds" % "4.1.0",
			"org.scalaj" %% "scalaj-http" % "2.3.0" % Test,
			"com.github.pathikrit" %% "better-files" % "3.4.0" % Test,
			"com.holdenkarau" %% "spark-testing-base" % "2.2.0_0.8.0" % Test
		)
  )

lazy val crisprJVM = crispr.jvm

lazy val crisprJS = crispr.js

