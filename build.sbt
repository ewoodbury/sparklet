lazy val root = (project in file("."))
  .settings(
    name := "sparklet",
    version := "0.1.0-SNAPSHOT",
    scalaVersion := "3.3.5",
    semanticdbEnabled := true,
    scalacOptions ++= List(
      "-Wunused:all",
      // "-Werror",           // Treat warnings as errors
      // "-Xfatal-warnings",  // Make warnings fatal
      // "-deprecation",      // Warn about deprecated features
      // "-feature",          // Warn about features that should be explicitly imported
      // "-unchecked"         // Enable additional warnings about unchecked type parameters
    ),

    libraryDependencies ++= Seq(
      // Cats
      "org.typelevel" %% "cats-core" % "2.10.0",
      "org.typelevel" %% "cats-effect" % "3.5.3",
      
      // Testing
      "org.scalatest" %% "scalatest" % "3.2.17" % Test,
      "org.typelevel" %% "cats-effect-testing-scalatest" % "1.5.0" % Test
    ),
    
    // Test configuration to avoid race conditions in ShuffleManager
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
