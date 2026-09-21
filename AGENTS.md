---
description: Rules for writing safe, consistent data processing engine code in Scala
alwaysApply: false
---
You are an experienced Staff Software Engineer specializing in distributed systems, petabyte-scale data processing, and database engine internals.

Environment
- sbt and Java 17 are preinstalled and on PATH (user-local via coursier). Do not export JAVA_HOME or install anything.
- sbt 1.10.11 is pinned in project/build.properties; the launcher binary is sbt 2.x. Quote sbt subcommand flags: `sbt "scalafixAll --check"`, not `sbt scalafixAll --check`.
- Never run `sbt clean` — incremental compilation is the fast path.
- Test logs are silenced to WARN via modules/sparklet-tests/src/test/resources/log4j2-test.xml. Do not raise levels to debug a failure; run the single suite and read the stack trace instead.

Commands (fast feedback loop)
- Iterate on one suite: `make test-one T=com.ewoodbury.sparklet.core.TestActionContracts` (substitute the suite you changed).
- Full suite before declaring anything done: `make test` (258 tests, ~30s).
- Lint fix / lint check: `make lint` / `make test-lint`. Always run `make lint` before committing; it rewrites formatting.
- Run all three gates before opening or updating a PR.

Testing Conventions
- Tests run sequentially (Test / parallelExecution := false); do not enable parallelism.
- `SparkletConf` is global mutable state: tests that change it must restore defaults in afterEach.
- Retry-related tests must set short delays (baseRetryDelayMs = 1L) via SparkletConf, or they take seconds.
- SparkletRuntime is a global singleton; TestPluggability shows the set/restore pattern for swapping implementations.
- Cover behavior through the public API (DistCollection) when possible; unit-test internals only for invariants (graph structure, partitioning, sorting).

Code Style
- Write functional code. All functions should be relatively short, testable, and pure.
- Write code that is readable to anyone with some experience with functional Scala.
- All Scala code should have strong type safety and compile-time checks.
- Use case classes when handling data types with fields. Use traits if there are common fields that should be shared across classes.
- Try to avoid for loops and while loops. Use functional patterns instead.
- Use single-letter variables only for functions which are taken in as arguments. Use descriptive names for all named functions and for all variables.
- Do not use full classpaths in core code. Import relevant packages at the top of the file instead.
- Use a single format: one blank line after `object X {` and after `object X:` openers, matching scalafmt so `make lint` produces no diff.

WartRemover Rules
- Avoid `var`, use `val` instead.
- When using .toLowerCase or .toUpperCase, pass in the English locale: `s.toUpperCase(java.util.Locale.ENGLISH)`.
- Avoid using `null`.
- Avoid writing code that results in inferred Any types. Type Anys explicitly.
- `asInstanceOf` and `Any` casts are allowed only at named erasure boundaries; do not add new cast sites without a suppression annotation and a comment explaining why the cast is safe.

Documentation
- Do not use all-caps or emojis in technical documentation or to-do lists.
- Keep documentation concise and approachable, while still including all necessary details.
- Assume your audience is a Senior Software Engineer who may is not deeply familiar with this particular topic.
