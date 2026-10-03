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
  // Type erasure policy: asInstanceOf and Any-inference are compile ERRORS. They are allowed
  // only at named erasure boundaries (Operation.fromPlan, StageBuilder, StageExecutor,
  // ShuffleHandler, JoinExecutor, DAGScheduler.executePartitions, DistCollection.kvPlan,
  // ColumnarPlanner, and the storage implementations), each carrying a suppression and a comment
  // explaining why the cast is safe. Do not add new cast sites outside those boundaries.
  wartremoverErrors ++= Seq(Wart.AsInstanceOf, Wart.Any),
  wartremoverWarnings ++= Warts.allBut(
    Wart.Any,
    Wart.AsInstanceOf,
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
    Wart.DefaultArguments,
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
lazy val `scarlet-api` = (project in file("modules/scarlet-api"))
  .settings(
    name := "scarlet-api",
    commonSettings,
    libraryDependencies ++= Seq(scalaLogging)
  )
  .dependsOn(`scarlet-core`)

// Core model and logical plan
lazy val `scarlet-core` = (project in file("modules/scarlet-core"))
  .settings(
    name := "scarlet-core",
    commonSettings,
    libraryDependencies ++= Seq(catsCore, catsEffect, scalaLogging, log4jApi, log4jCore, log4jSlf4j, disruptor)
  )

// Runtime SPI (RunnableTask, TaskScheduler, ExecutorBackend, Partitioner, ShuffleService)
lazy val `scarlet-runtime` = (project in file("modules/scarlet-runtime"))
  .settings(
    name := "scarlet-runtime",
    commonSettings,
    libraryDependencies ++= Seq(scalaLogging)
  )
  .dependsOn(`scarlet-core`)

// Execution engine (Stage, Task, DAGScheduler, StageBuilder, DistCollection, Executor)
lazy val `scarlet-execution` = (project in file("modules/scarlet-execution"))
  .settings(
    name := "scarlet-execution",
    commonSettings,
    libraryDependencies ++= Seq(catsCore, catsEffect, scalaLogging, log4jApi, log4jCore, log4jSlf4j, disruptor)
  )
  .dependsOn(`scarlet-api`, `scarlet-core`, `scarlet-runtime`, `scarlet-columnar`)

// Column batches. No dependency on the logical plan or the row executor.
lazy val `scarlet-columnar` = (project in file("modules/scarlet-columnar"))
  .settings(
    name := "scarlet-columnar",
    commonSettings
  )

// Test module aggregating all tests
lazy val `scarlet-tests` = (project in file("modules/scarlet-tests"))
  .settings(
    name := "scarlet-tests",
    commonSettings,
    libraryDependencies ++= Seq(scalatest % Test, ceTesting % Test)
  )
  .dependsOn(`scarlet-api`, `scarlet-execution`, `scarlet-runtime`, `scarlet-columnar`)

// Root aggregator
lazy val root = (project in file("."))
  .aggregate(
    `scarlet-api`,
    `scarlet-core`,
    `scarlet-runtime`,
    `scarlet-execution`,
    `scarlet-columnar`,
    `scarlet-tests`
  )
  .settings(
    name := "scarlet",
    publish / skip := true,
    Compile / sources := Seq.empty,
    Test / sources := Seq.empty,
    Compile / resourceDirectories := Nil,
    Test / resourceDirectories := Nil
  )
