# Distributed Systems Project 2026
![Java](https://img.shields.io/badge/Java-ED8B00?style=flat-square&logo=java&logoColor=white)
![Akka](https://img.shields.io/badge/Akka-15A9CE?style=flat-square&logo=akka&logoColor=white)
![Gradle](https://img.shields.io/badge/Gradle-Build-02303A?style=flat-square&logo=gradle&logoColor=white)

Repository for the **Distributed Systems** project assigned in the academic year **2025–2026**.  
The project is implemented in **Java** using **Akka Actors**.

The project requires **JDK 25** (LTS). The Gradle build selects Java 25 for compilation, tests, and application runs; if no JDK 25 is installed, Gradle downloads one automatically.

## Setting up Gradle

To avoid version issues, you are required to use Gradle `9.2.1`.  
The easiest way to achieve this is by using a wrapper:

```bash
gradle wrapper --gradle-version 9.2.1
```

If you have an old version of Gradle, you can update it running

```bash
curl -s "https://get.sdkman.io" | bash
source "$HOME/.sdkman/bin/sdkman-init.sh"
sdk install gradle 9.2.1
```

## Build, Run, and Tets the Project

```bash
./gradlew build
./gradlew run
./gradlew test
```

## Regression Testing suite
To run regression tests use the command:
```bash
./gradlew regression
```
or 
```bash
./gradlew test --tests Regression
```

Every new functionality that is implemented must be accompanied by its tests. 

### Adding tests to the regression suite
For adding test to the regression suite please take a look on the `src/test/java/it/unitn/ds/regression/Regression.java` file, here a class `Regression` is defined. Inside this class you can add your tests, for example:
```java
@Test
public void assertTrueTest() {
    assertTrue(true);
}
```

Note that to use it with Akka you need to use the `TestKit` class, for example:
```java
@Test
public void testClientReadRequest() {
    ActorSystem system = ActorSystem.create("TestSystem");
    TestKit testProbe = new TestKit(system) {{
        final ActorRef client = system.actorOf(Client.propsWithListener(getRef(), 0, 0, 0, 0, 0, 0));
        ....        
    }};
}
```

## Test Coverage

```bash
./gradlew regressionCoverage    # our own tests only — fast
./gradlew testCoverage          # everything: base suite + regression — slow
```

Each task prints the path of its report when it finishes.

JaCoCo instruments the bytecode and records which lines and branches the tests actually
execute. It finds no bugs: it tells you **where you have not looked**. On a protocol with
crashes and message reordering the uncovered lines are the error paths, which is exactly
where bugs survive — so the report is a way to choose which test to write next instead of
guessing.

`testCoverage` is the complete picture: `test` has no package filter, so it runs the base
suite *and* our regression tests. `regressionCoverage` is the fast subset, and the one for
day to day — `testCoverage` takes about ten minutes because the base suite waits out its
election timeouts.

`AbstractClient`, `AbstractReplica`, `Logger`, `NetworkChannel` and `Main` are excluded:
they come from the course template, and measuring our coverage of somebody else's
infrastructure only dilutes the number.

JaCoCo is pinned to 0.8.15 — earlier versions cannot instrument Java 25 class files and
abort with `IllegalClassFormatException`.

## Static Code Analysis

The build integrates five tools, each answering a question the others cannot:

| Tool | What it looks at |
|---|---|
| **PMD** | Source AST: code smells, naming, missing Javadoc |
| **SpotBugs** | Bytecode dataflow: probable bugs, thread visibility, overflow |
| **CPD** | Duplicated blocks (token-based, survives renames) |
| **ArchUnit** | Architectural rules, expressed as tests |
| **Spotless** | Formatting — `spotlessApply` rewrites, `staticAnalysis` reports |

PMD and SpotBugs run with `ignoreFailures = true`: they report findings but never turn the
build red. Tighten that once the counts are low enough to be sustainable.

**Only our own code is analyzed.** `it.unitn.ds.base` and `TestsCommons` come from the
course template and we do not get to change them, so findings there would be
unactionable; `pmdTest` and `spotbugsTest` are restricted to `it.unitn.ds.regression` and
`it.unitn.ds.contract` in `build.gradle`.

### Running the analysis

```bash
./gradlew staticAnalysis      # PMD + SpotBugs + CPD + formatting — the daily one
./gradlew cpd                 # duplication only
./gradlew callbackContract    # the callback checklist (see below)
./gradlew spotlessApply       # reformat
./gradlew check               # tests + PMD + SpotBugs; slow
```

`staticAnalysis` runs every analyser and prints each finding as
`<file>:<line>: <RULE>: <message>`, which most terminals and editors turn into a clickable
link. Duplications get one line per end of the block, so both are navigable; unformatted
files are anchored on the first line that differs. The run closes with a count:

```
Errors: 0   Warnings: 0   Info: 0   Duplications: 0   Formatting: 0   (0 problems)
```

Errors, warnings and info come from the tools' own severities. Duplications and formatting
are counted apart — they are not rule violations, but they are still entries in the
Problems panel, so they are in the total.

Reports on disk:

| Path | Content |
|---|---|
| `build/reports/pmd/` | HTML + XML |
| `build/reports/spotbugs/` | HTML + SARIF |
| `build/reports/cpd/cpd.xml` | Duplications |
| `build/reports/jacoco/<task>/html/` | Coverage |

PMD and SpotBugs always re-run rather than going `UP-TO-DATE`. An up-to-date task prints
nothing, which an editor reads as "no findings" and uses to clear the Problems panel — a
silently emptied panel is worse than a few seconds of re-analysis.

### The callback contract

`src/test/java/it/unitn/ds/contract/TestCallbackContract.java` checks, via ArchUnit, that
every `callbackOn*` method of `AbstractClient` and `AbstractReplica` is actually invoked
somewhere. The graders' tests observe our system *only* through those callbacks: forgetting
one makes a correct implementation look broken, and nothing else in the build notices.

The check is **static** — it reads the compiled bytecode looking for the call, so it catches
"you never call it", not "you call it at the wrong moment". It asks whether anything in
`it.unitn.ds` makes the call rather than pinning it on `Client` or `Replica`, because a
callback is likely to be fired from a `Transaction` subclass, and ArchUnit sees only the
calls in a class's own bytecode.

These tests are tagged `contract` and **excluded from both `test` and `regression`**: they
stay red until the last callback is implemented, and a permanently red CI is a CI nobody
reads. Run `./gradlew callbackContract` deliberately, towards the end of development.

### Formatting

```bash
./gradlew spotlessApply     # rewrites the files — run it before committing
```

It reformats, drops unused imports, trims trailing whitespace and adds the final newline.

There is no check task: `spotlessCheck` is disabled, because `staticAnalysis` already
reports unformatted files as findings. A second path that fails the build instead of
reporting would only get in the way.

### VS Code integration

`.vscode/tasks.json` defines six tasks. Each one attaches a **problem matcher**, which is
what makes findings appear as **squiggles on the offending line**, with the message on
hover, an entry in the Problems panel, and `F8` / `Shift+F8` to jump between them. You
never have to read the terminal.

| Task | What it does |
|---|---|
| **Static analysis (PMD + SpotBugs)** | One-shot analysis; findings become warnings |
| **Static analysis (watch)** | Same, re-run automatically on every save |
| **Run tests** | `./gradlew test`; failures become errors on the failing assertion |
| **Run regression tests** | Same for the regression suite |
| **Duplicated code (CPD)** | Duplications become hints on both ends of each block |
| **Clear analysis problems** | Empties the Problems panel without re-running anything |

`callbackContract` has no VS Code task: its output is a checklist to read, not a set of
locations to jump between.

Run them with `Ctrl+Shift+P` → *Tasks: Run Task*. **Run tests** is the default test task,
so `Ctrl+Shift+P` → *Tasks: Run Test Task* goes straight to it.

Important: this only works when launched **as a VS Code task**. Running `./gradlew` in a
terminal produces the same text, but VS Code only feeds a problem matcher from a task it
started itself — from a plain terminal you just get `Ctrl+Click`-able paths.

**Watch mode** is the one worth keeping running: it uses Gradle's `--continuous` flag, so
saving any source file re-runs the analysis and refreshes the squiggles. VS Code needs to
know where one pass ends and the next begins, in order to clear the previous diagnostics
before publishing the new ones; it takes that from Gradle's own output (`> Task
:compileJava` opens a pass, `BUILD SUCCESSFUL` / `Waiting for changes` closes it).

**Test failures.** Gradle normally reports a failure as a stack trace, whose top frames
belong to JUnit, Akka and Scala. An `afterTest` hook on every `Test` task takes the first
stack frame under `it.unitn.ds` — the assertion you actually wrote — and prints it in the
format the matcher understands, so a failing test underlines the failing line. The hook
covers `regression` too, since it is registered on all `Test` tasks.

**Other ways to launch.** Since you have `vscjava.vscode-gradle`, the Gradle sidebar shows
`staticAnalysis` under *verification* with a play button — one click, but that extension
runs it in its own terminal, so no squiggles. To bind the task to a key instead, add this
to your user `keybindings.json` (VS Code has no per-workspace keybindings):

```json
{
    "key": "ctrl+alt+a",
    "command": "workbench.action.tasks.runTask",
    "args": "Static analysis (PMD + SpotBugs)"
}
```

> **Note:** `.vscode/` is listed in `.gitignore`, but `tasks.json` has been force-added and
> *is* tracked, so a fresh clone gets the tasks. Anything else you drop in `.vscode/`
> (`settings.json`, for instance) stays local unless you add it explicitly with
> `git add -f`.

### Configuration

| File | Purpose |
|---|---|
| `config/pmd/ruleset.xml` | Which PMD rules are active |
| `config/spotbugs/exclude.xml` | Which SpotBugs detectors are silenced |

Tool versions are pinned in `build.gradle` (`pmd.toolVersion`, `spotbugs.toolVersion`,
`jacoco.toolVersion`).

SpotBugs runs at `reportLevel = LOW`. That setting is about **confidence**, not severity:
it controls how sure a detector must be before reporting. `LOW` reports everything the
detectors suspect, which here costs seven findings over the `MEDIUM` default — one of them
a real bug. Seeing them and deciding beats filtering them out upstream.

**Exclusions are a starting point, not a verdict.** A rule exists for a reason, and
silencing one means accepting the risk it was guarding against. Every exclusion carries an
inline comment explaining why; if you want to see what is behind one, delete it and re-run.

`scratch/StaticAnalysis_Exclusions.md` documents each one: what the rule detects, why the
pattern is generally a problem, what it actually flags here, and whether the exclusion
holds up. Most of the original exclusions were removed by fixing the code instead — that
document records which, and why the remaining ones stay.

## Repository Structure

In the `main/java/it/unitn/ds` directory you will find the project base classes.  
`AbstractClient` and `AbstractReplica` contain the logic and structure that allows automated tests to work.  
Your implementation **must** use as base classes `Client` and `Replica` (which already inherit from their corresponding abstract classes).

### Logs

Please, **DO NOT print directly to standard output or file**. Instead, use the provided `Logger` class. Use the `log(String)` function to print "official" logs. Use `debug(String)` for debug prints. Both `AbstractClient` and `AbstractReplica` provide `log` and `debug` wrappers that prepend to your logs the string `[Client/Replica <ID>]` (resp.).

*Not adhering to this may cause automated tests to fail, even if your code works perfectly. This is due to prints slowing down you code. Tests are based on strict time intervals.*

## Client

As enforced by the abstract class, your `Client` class **must** implement the following methods:
- `public void sendRead(ActorRef replica, int index)`
- `public void sendWrite(ActorRef replica, int index, int value)`

During tests, these methods will be automatically invoked by the abstract class.  
In you test main function, if you want a client to send a message, do the following:
```java
int index = 0;
client.tell(new AbstractClient.ReadRequest(index), Actor.noSender());
```

For complete examples, please refer to `test/java/it/unitn/ds/base/APICompliance.java`.

To ensure automated tests can detect events in you system you **must** invoke the `AbstractClient.callback*(...)` methods whenever required, depending on the semantic of the callback.  
Follows a complete list of client-side callbacks. You can find more info in the related Java docs in the code.

- `callbackOnReadResult(AbstractClient.ReadResult)`
- `callbackOnWriteResult(AbstractClient.WriteResult)`
- `callbackOnReadTimeout(AbstractClient.ReadTimeout)`
- `callbackOnWriteTimeout(AbstractClient.WriteTimeout)`

## Replica

Analogously to `Client` your `Replica` implementation **must** implement some methods and invoke callbacks.

*Methods*

- `public int getSystemNumberOfActors()`
- `public void crash(AbstractReplica.Crash how)`
- `public void initSystem(AbstractReplica.InitSystem sysInit)`

*Callbacks*

- `callbackOnUpdateApplied(int index, int value)`
- `callbackOnElectionStarted(int crashedCoordId)`
- `callbackOnCoordinatorElected(int newCoordID)`

As for the client, you can find more documentation in the Java docs.

The size of positions list **must** be equal to `AbstractReplica.POSITIONS_LIST_LENGTH`.

Coordinators **must** send heartbeats every `AbstractReplica.getCoordinatorBeatInterval()` milliseconds.

### Emulated Network Delays

Automated tests expect to see particular messages after calculated time intervals. These intervals are computed based on `AbstractReplica.getMinLatency()` and `AbstractReplica.getMaxLatency()` (these latencies will be set by the `AbstractReplica` class constructor). It is therefore fundamental that your replica implementation conforms to these delays. Delays are always expressed in MILLISECONDS.

`AbstractReplica` provides a `void tell(Serializable m, ActorRef dst)` method which emulates network latency ensuring messages are delivered in FIFO order (`NetworkChannel` class). **Not using this method may cause some tests to fail (even if the implementation is correct).** 

*Hint: in your replica implementation, create the `tell`, `broadcast`, and `multicast` functions to handle messages. This way, you will unify the handling of crashes.*

## How do tests work?

In the `test/java/it/unitn/ds/base` directory you can find some basic tests that allow to verify basic system functionalities. First of all, refer to `APICompliance.java`.

In general, each test works as follows:
1) Initialize a new system using the `AbstractReplica.InitSystem` message.
2) Whenever an actor (`Client` or `Replica`) is instantiated, the test passes to it a `probe` (or `listener`), which is a special kind of actor (`akka.testkit.javadsl.TestKit`). This is achieved via the `Client/Replica.propsWithListener(...)` method.
3) The test will send read/write requests and emulate crashes using the abstract methods and pre-defined message classes of clients and replicas.
4) The `callback*(...)` functions will forward messages to the `probe` which will then be able to verify that the system behaves as intended.

In summary:
1) Abstract classes ensure test have a standardized way to send read and write requests from clients and emulate crashes.
2) Properly invoked callbacks ensure that probes receive the required information to verify the system's behavior.

Thus, **it is crucial that you follow `Client` and `Replica` guidelines** (described above).

**Before you submit you project, all base tests must pass.**  
*Submissions that do not meet this requirement will be ignored.*

## Note

This is the first time we automate the tests. If you find issues in the base classes or base tests, let us know via email or telegram group.

## Contributors

- Stefano Genetti [stefano.genetti@unitn.it]
- Thomas Pasquali [thomas.pasquali@unitn.it]
