# Observability, Testing, and Verification Infrastructure

> **Learning goal.** Learn how the repository makes actor behavior observable, what each test/build tool proves, and how to avoid claiming more validation than the evidence supports.

**Build snapshot:** `feature/electionTransaction` at `d1222a2`.  
**Key files:** `Logger.java`, abstract callbacks, `TestsCommons.java`, base/regression/contract tests, `build.gradle`, `.github/workflows/ci.yml`, `charts/`.

## 1. Why distributed tests need observability

Most important state lives inside actors and cannot be read directly. Messages also arrive asynchronously. A test therefore needs stable public observations:

- client result/timeout callbacks;
- replica update/election callbacks;
- TestKit probes receiving those callback objects;
- protocol messages captured by probes;
- logs for human traces; and
- bounded waiting based on configured timing.

Testing only private fields would bypass the actor model and give false confidence about real message flows.

## 2. Logger contract

The supplied `Logger` produces timestamped INFO/DEBUG lines to stdout or a file. `AbstractClient` and `AbstractReplica` prepend actor identity.

Direct `System.out.println` inside hot protocol paths is discouraged because strict timing tests can be disturbed by slow I/O. Debug logging can be disabled, and all logging can be disabled during tests.

`Logger` synchronizes access to its shared writer. This static lock is infrastructure-level shared state; protocol application state remains actor-owned.

Logs help answer “what happened?” but do not prove correctness. Missing or reordered log output can also reflect buffering/timing rather than protocol state.

## 3. Mandatory callbacks

Client callbacks:

- read result;
- write result;
- read timeout;
- write timeout.

Replica callbacks:

- update applied;
- election started;
- coordinator elected.

Each callback logs and forwards a small immutable result object to the optional listener. Tests pass a TestKit actor as listener and assert those events.

Callback timing is semantic. For example, `callbackOnUpdateApplied` belongs immediately after actual local `positions[]` mutation, not merely after receiving UPDATE.

## 4. TestKit mental model

A TestKit probe is a controlled actor mailbox for a test. The system sends it ordinary messages; test code asks for the next message of a class, fishes for a matching message, or asserts no message arrives for a duration.

Useful patterns:

```text
expectMsgClass(Result.class)     wait for one result
expectNoMessage(duration)       assert silence in a window
fishForMessage(...)             ignore unrelated events until target
```

Negative timing assertions must be chosen carefully. “No message for 50 ms” proves little if the valid path can take 100 ms.

## 5. `TestsCommons`

`createTestSystem`:

1. creates an `ActorSystem`;
2. creates one listener probe per replica;
3. creates replicas with configurable latency/heartbeat interval;
4. sends `InitSystem` to all; and
5. returns actors, probes, and derived timeout values.

Timing helpers estimate read, write, update, and election windows from max latency, replica count, and heartbeat interval. These are test budgets, not formal proofs of worst-case Akka scheduling.

`getMaxUpdateDelay` comments model four logical hops: client to replica, replica to coordinator, coordinator broadcast/ACK round, and WRITEOK/result progression, with jitter allowance.

## 6. Three test layers

### Course base tests

`src/test/java/it/unitn/ds/base` is supplied contract/integration coverage. `APICompliance` checks required construction and API behavior. `NoCrashes` and `WithCrashes` run longer system scenarios.

These are the acceptance boundary: the README says all base tests must pass.

### Project regression tests

`src/test/java/it/unitn/ds/regression` contains focused tests written during development:

- message equality;
- epoch comparison;
- dispatcher routing;
- read/write wrappers;
- update feature behavior;
- heartbeat behavior; and
- election data structures/validation.

Regression tests should be fast and isolate one invariant or bug.

### Callback contract tests

`TestCallbackContract` uses ArchUnit bytecode inspection to ask whether any project class calls every mandatory callback.

This is a static checklist. It catches “callback is never called anywhere.” It does not catch wrong timing, wrong arguments, duplicate calls, or unreachable paths.

The task is deliberately separate because callbacks for unfinished features keep it red during development.

## 7. Gradle test tasks

- `./gradlew test`: base plus regression tests, excluding contract tag.
- `./gradlew regression`: only project regression package.
- `./gradlew callbackContract`: static callback checklist.
- `./gradlew regressionCoverage`: JaCoCo for the fast regression suite.
- `./gradlew testCoverage`: complete base and regression coverage.

The build requires Java 25 and configures the toolchain resolver. The CI workflow installs Java 25 and Gradle 9.2.1, creates the wrapper, then runs regression tests for pull requests to main.

CI evidence belongs to one exact commit. A green feature-branch check does not prove a later merge commit.

## 8. Coverage

JaCoCo measures executed lines and branches. It does not determine whether assertions are correct or whether protocol properties hold.

Coverage is most useful for finding unvisited failure paths:

- stale timeout branch;
- wrong coordinator filter;
- duplicate message branch;
- election skip path; and
- synchronization rejection.

Template infrastructure is excluded so project coverage is not diluted by supplied code.

High coverage with weak assertions can still miss a broken protocol. Low coverage reliably identifies places nobody tested.

## 9. Static analysis suite

The build integrates:

- **PMD:** source-level smells/rules;
- **SpotBugs:** bytecode dataflow suspicions;
- **CPD:** duplicated token blocks;
- **Spotless:** formatting output/check support;
- **ArchUnit:** callback architecture rules.

`staticAnalysis` combines reports and prints editor-clickable findings. PMD and SpotBugs use `ignoreFailures = true`, meaning task completion does not mean zero findings. The final count and report files must be inspected.

Static analysis can find probable local bugs. It cannot prove quorum, total order, uniform agreement, or election liveness.

## 10. VS Code integration

Tracked tasks attach problem matchers so tests and analysis findings appear at source lines. Continuous analysis re-runs on save.

This is developer ergonomics, not runtime architecture, but it affects how reliably findings are noticed. Running a Gradle command in an ordinary terminal may produce the same text without VS Code diagnostics.

## 11. Protocol diagrams

`charts/` contains:

- system and actor UML;
- transaction/message UML;
- healthy and failure sequences;
- heartbeat coordinator/follower FSMs;
- election sequence/FSM; and
- intended read/write/update traces.

Diagrams are explanatory artifacts, not automatically generated truth. Compare every arrow with current constructors, receive handlers, network/local paths, and branch status. Some diagrams describe intended update behavior not integrated in the current source.

## 12. Evidence ladder

From weakest to strongest for a behavior claim:

1. comment or TODO describes intention;
2. diagram shows proposed flow;
3. code contains a path;
4. focused test executes the path with meaningful assertions;
5. integration test exercises interacting actors/failures;
6. exact commit passes required suite/CI;
7. correctness argument connects tested mechanisms to stated assumptions.

No single level replaces all others.

## 13. Current validation snapshot

Prior recorded validation for election commit `d1222a2` showed focused election and regression tasks passing, while callback contract still had unrelated missing read/update callbacks and static analysis reported findings. That evidence is historical to this commit and environment.

For these documentation PDFs, source/render validity is checked separately; the task does not modify Java behavior or claim a fresh full protocol test pass.

## 14. Building a good protocol test

A strong test names one invariant, arranges a concrete state, triggers one event sequence, and checks externally visible effects plus forbidden effects.

Example stale watchdog test:

1. initialize follower with short heartbeat interval;
2. capture watchdog version 1 event;
3. deliver valid heartbeat, causing version 2;
4. inject expiry for version 1;
5. assert no election callback;
6. inject expiry for version 2;
7. assert exactly one election callback.

This demonstrates why the version check matters, not merely that lines execute.

## 15. Codebase coverage map

This report set assigns every maintained source area to an educational report:

- `Main`, `AbstractClient`, `AbstractReplica`, and actor factories: reports 01 and 05.
- `NetworkChannel` and replica send helpers: report 02.
- `DistributedActor`, `Transaction`, `Msg`, `EpochPair`, dispatchers, and `ProbeTransaction`: reports 03 and 04.
- `Client`: reports 05–07.
- branch-only `ReadTransaction`: report 06.
- `WriteTransaction`: report 07.
- branch-only `UpdateTransaction`: report 08.
- `HeartbeatTransaction`: report 09.
- crash fields/handlers across abstract and concrete replica code: report 10.
- `ElectionTransaction` and election-specific `Replica` handlers: report 11.
- synchronization and heartbeat replacement in `Replica`: report 12.
- `Logger`, `TestsCommons`, base/regression/contract tests, Gradle tasks, CI, PMD/SpotBugs configuration, and VS Code tasks: this report.
- the specification PDFs and all cross-protocol guarantees: reports 00 and 14.
- `charts/UML`, `charts/Transactions`, and `charts/protocol.mmd`: referenced by the matching architecture/protocol reports and assessed here as documentation evidence.
- `docs/ELECTION_TRANSACTION_PLAN.md`: election working notes; useful historical context, not automatically current behavior.

Generated `.gradle/` and `build/` contents are build outputs rather than maintained subsystem source. They are evidence only when a report explicitly cites a generated test, coverage, PMD, SpotBugs, CPD, or JaCoCo result.

## 16. Exam rehearsal

**What does callbackContract prove?** Only that compiled project code invokes each callback somewhere.

**What does green regression CI prove?** The configured regression task passed on that exact checked-out commit/environment; it does not prove untested protocol properties or later integration.

The invariant to remember is: **every correctness claim should name both the mechanism in code and the test or reasoning evidence that supports it.**

## 17. The evidence pipeline

<pre class="diagram">source code -> focused unit test -> regression scenario
     |                 |                    |
     +--> callback/log/probe observation ---+
                           |
                    claim with limits
                           |
                 build/SCA/CI evidence (when available)</pre>

Think of a test as an instrument, not a magic stamp. A focused test can show
that a timeout message is routed to the right transaction. A regression test
can show that a five-replica write eventually completes. Neither alone proves
all crash interleavings. The report should always state the observation and the
unobserved cases.

## 18. Code microscope: read a TestKit test backwards

Start with the final assertion and ask what mechanism could make it true. If a
test expects a probe to receive `CallbackOnCoordinatorElected`, trace backwards:

1. which actor sent the callback;
2. which election state called that sender;
3. which timeout or ACK caused the transition;
4. which fixture supplied membership, delays, and crash status; and
5. whether the test checked exactly-once behavior or merely eventual presence.

<pre class="code-microscope">// Conceptual assertion anatomy
probe.expectMsg(CoordinatorElected(id));
// proves an observable event, not automatically:
// - all replicas installed the same epoch
// - no duplicate callback was emitted
// - pending writes were recovered</pre>

This backwards reading habit is especially useful with `TestDispatcher` and
`TestsCommons`, where helper methods hide actor creation and timing settings.
Open the helper before trusting what a short test appears to configure.

## 19. Designing a high-value protocol test

Name one invariant, one perturbation, and one observable result. Example:

<table><tr><th>Invariant</th><th>Perturbation</th><th>Observable result</th></tr>
<tr><td>duplicate WRITEOK is idempotent</td><td>deliver the same message twice</td><td>one history entry and one state mutation</td></tr>
<tr><td>stale watchdog is harmless</td><td>deliver v6 after v7</td><td>no election callback for v6</td></tr>
<tr><td>queue preserves session order</td><td>enqueue write then read</td><td>read starts only after write completion</td></tr></table>

Add a negative assertion where possible: “probe receives no second callback
within the bounded test window.” Avoid arbitrary sleeps; derive the window from
configured channel delay plus protocol hops and explain the margin.

### Practice

Choose one claim from each of the read, heartbeat, election, and recovery
reports. For each, write what a unit test can establish and what integration or
formal reasoning is still needed.
