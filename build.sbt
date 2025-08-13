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
    Wart.ExplicitImplicitTypes
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

// Core model and logical plan
lazy val `sparklet-core` = (project in file("modules/sparklet-core"))
  .settings(
    name := "sparklet-core",
    commonSettings,
    libraryDependencies ++= Seq(scalaLogging)
  )

// Runtime SPI (RunnableTask, TaskScheduler, ExecutorBackend, Partitioner, ShuffleService)
lazy val `sparklet-runtime-api` = (project in file("modules/sparklet-runtime-api"))
  .settings(
    name := "sparklet-runtime-api",
    commonSettings,
    libraryDependencies ++= Seq(scalaLogging)
  )
  .dependsOn(`sparklet-core`)

// Local runtime implementations (TaskScheduler, ExecutorBackend)
lazy val `sparklet-runtime-local` = (project in file("modules/sparklet-runtime-local"))
  .settings(
    name := "sparklet-runtime-local",
    commonSettings,
    libraryDependencies ++= Seq(catsCore, catsEffect, scalaLogging, log4jApi, log4jCore, log4jSlf4j, disruptor)
  )
  .dependsOn(`sparklet-core`, `sparklet-runtime-api`)

// Local shuffle + partitioner implementations
lazy val `sparklet-shuffle-local` = (project in file("modules/sparklet-shuffle-local"))
  .settings(
    name := "sparklet-shuffle-local",
    commonSettings,
    libraryDependencies ++= Seq(scalaLogging)
  )
  .dependsOn(`sparklet-core`, `sparklet-runtime-api`)

// Default runtime wiring that picks local implementations
lazy val `sparklet-runtime-default` = (project in file("modules/sparklet-runtime-default"))
  .settings(
    name := "sparklet-runtime-default",
    commonSettings,
    libraryDependencies ++= Seq(catsCore, catsEffect, scalaLogging)
  )
  .dependsOn(`sparklet-core`, `sparklet-runtime-api`, `sparklet-runtime-local`, `sparklet-shuffle-local`)

// Execution engine (Stage, Task, DAGScheduler, StageBuilder, DistCollection, Executor)
lazy val `sparklet-execution` = (project in file("modules/sparklet-execution"))
  .settings(
    name := "sparklet-execution",
    commonSettings,
    libraryDependencies ++= Seq(catsCore, catsEffect, scalaLogging, log4jApi, log4jCore, log4jSlf4j, disruptor)
  )
  .dependsOn(`sparklet-core`, `sparklet-runtime-api`, `sparklet-runtime-default`)

// Test module aggregating all tests
lazy val `sparklet-tests` = (project in file("modules/sparklet-tests"))
  .settings(
    name := "sparklet-tests",
    commonSettings,
    libraryDependencies ++= Seq(scalatest % Test, ceTesting % Test)
  )
  .dependsOn(`sparklet-execution`, `sparklet-runtime-default`, `sparklet-runtime-local`, `sparklet-shuffle-local`)

// Root aggregator
lazy val root = (project in file("."))
  .aggregate(
    `sparklet-core`,
    `sparklet-runtime-api`,
    `sparklet-runtime-local`,
    `sparklet-shuffle-local`,
    `sparklet-runtime-default`,
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
