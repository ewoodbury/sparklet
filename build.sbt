ThisBuild / organization := "com.ewoodbury"
ThisBuild / scalaVersion := "3.3.5"
ThisBuild / version := "0.1.0-SNAPSHOT"

lazy val commonSettings = Seq(
  semanticdbEnabled := true,
  scalacOptions ++= List(
    "-Wunused:all"
  ),
  // Prefer async loggers
  ThisBuild / javaOptions ++= Seq(
    "-DLog4jContextSelector=org.apache.logging.log4j.core.async.AsyncLoggerContextSelector"
  ),
  // Run tests sequentially to avoid cross-suite state interference
  Test / parallelExecution := false,
  // Wartremover configuration
  wartremoverWarnings ++= Warts.allBut(
    Wart.ImplicitParameter,
    Wart.Overloading,
    Wart.NonUnitStatements,
    Wart.Throw,
    Wart.While,
    Wart.Return,
    Wart.AsInstanceOf,
    Wart.IsInstanceOf,
    Wart.OptionPartial,
    Wart.TryPartial,
    Wart.EitherProjectionPartial,
    Wart.ArrayEquals,
    Wart.ImplicitConversion,
    Wart.Serializable,
    Wart.JavaSerializable,
    Wart.Product,
    Wart.LeakingSealed,
    Wart.PublicInference,
    Wart.Option2Iterable,
    Wart.StringPlusAny,
    Wart.JavaConversions,
    Wart.Recursion,
    Wart.Enumeration,
    Wart.ExplicitImplicitTypes,
    Wart.SizeIs,
  )
)

lazy val catsCore = "org.typelevel" %% "cats-core" % "2.10.0"
lazy val catsEffect = "org.typelevel" %% "cats-effect" % "3.5.3"
lazy val scalaLogging = "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5"
lazy val log4jApi = "org.apache.logging.log4j" % "log4j-api" % "2.23.1"
lazy val log4jCore = "org.apache.logging.log4j" % "log4j-core" % "2.23.1"
lazy val log4jSlf4j = "org.apache.logging.log4j" % "log4j-slf4j2-impl" % "2.23.1"
lazy val disruptor = "com.lmax" % "disruptor" % "3.4.4" // required for Log4j2 async

lazy val scalatest = "org.scalatest" %% "scalatest" % "3.2.17"
lazy val ceTesting = "org.typelevel" %% "cats-effect-testing-scalatest" % "1.5.0"

// User-facing API (clean interface)
lazy val `sparklet-api` = (project in file("modules/sparklet-api"))
  .settings(
    name := "sparklet-api",
    commonSettings,
    libraryDependencies ++= Seq(scalaLogging)
  )
  .dependsOn(`sparklet-core`)

// Core model and logical plan
lazy val `sparklet-core` = (project in file("modules/sparklet-core"))
  .settings(
    name := "sparklet-core",
    commonSettings,
    libraryDependencies ++= Seq(catsCore, catsEffect, scalaLogging, log4jApi, log4jCore, log4jSlf4j, disruptor)
  )

// Runtime SPI (RunnableTask, TaskScheduler, ExecutorBackend, Partitioner, ShuffleService)
lazy val `sparklet-runtime` = (project in file("modules/sparklet-runtime"))
  .settings(
    name := "sparklet-runtime",
    commonSettings,
    libraryDependencies ++= Seq(scalaLogging)
  )
  .dependsOn(`sparklet-core`)

// Execution engine (Stage, Task, DAGScheduler, StageBuilder, DistCollection, Executor)
lazy val `sparklet-execution` = (project in file("modules/sparklet-execution"))
  .settings(
    name := "sparklet-execution",
    commonSettings,
    libraryDependencies ++= Seq(catsCore, catsEffect, scalaLogging, log4jApi, log4jCore, log4jSlf4j, disruptor)
  )
  .dependsOn(`sparklet-api`, `sparklet-core`, `sparklet-runtime`)

// Test module aggregating all tests
lazy val `sparklet-tests` = (project in file("modules/sparklet-tests"))
  .settings(
    name := "sparklet-tests",
    commonSettings,
    libraryDependencies ++= Seq(scalatest % Test, ceTesting % Test)
  )
  .dependsOn(`sparklet-api`, `sparklet-execution`, `sparklet-runtime`)

// Root aggregator
lazy val root = (project in file("."))
  .aggregate(
    `sparklet-api`,
    `sparklet-core`,
    `sparklet-runtime`,
    `sparklet-execution`,
    `sparklet-tests`
  )
  .settings(
    name := "sparklet",
    publish / skip := true,
    Compile / sources := Seq.empty,
    Test / sources := Seq.empty,
    Compile / resourceDirectories := Nil,
    Test / resourceDirectories := Nil
  )
