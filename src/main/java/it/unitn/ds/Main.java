package it.unitn.ds;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import it.unitn.ds.AbstractClient.ReadRequest;
import it.unitn.ds.AbstractClient.ReadResult;
import it.unitn.ds.AbstractClient.ReadTimeout;
import it.unitn.ds.AbstractClient.WriteRequest;
import it.unitn.ds.AbstractClient.WriteResult;
import it.unitn.ds.AbstractClient.WriteTimeout;
import it.unitn.ds.AbstractReplica.CoordinatorElected;
import it.unitn.ds.AbstractReplica.Crash;
import it.unitn.ds.AbstractReplica.ElectionStarted;
import it.unitn.ds.AbstractReplica.InitSystem;
import it.unitn.ds.AbstractReplica.UpdateApplied;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * A few small, runnable scenarios for looking at the replicated storage protocol.
 *
 * <p>Run everything with {@code ./gradlew run}, or select one example with
 * {@code ./gradlew run --args=concurrent}. These scenarios use the same public
 * messages as normal clients, so the logs show the real protocol in action.</p>
 */
public final class Main {

    private static final int INDEX = 0;
    private static final int COORDINATOR_ID = 0;
    private static final long CLIENT_READ_TIMEOUT = 2_000L;
    private static final long CLIENT_WRITE_TIMEOUT = 18_000L;
    private static final long SETUP_DELAY_MILLIS = 250L;

    private Main() {}

    public static void main(String[] args) throws Exception {
        Logger.setDestinationStdout();
        Logger.setLoggingEnabled(true);
        Logger.setDebugEnabled(false);

        String requestedExample = args.length == 0 ? "all" : args[0].toLowerCase();
        Logger.log("[Main] Starting example(s): " + requestedExample);

        switch (requestedExample) {
            case "all" -> runAllExamples();
            case "basic" -> runBasicReplicationExample();
            case "concurrent" -> runConcurrentWorkloadExample();
            case "majority" -> runStrictMajorityExample();
            case "election" -> runCoordinatorElectionExample();
            case "timeout" -> runCrashedReplicaReadExample();
            default -> {
                Logger.log("[Main] Unknown example '" + requestedExample + "'.");
                Logger.log("[Main] Choose: all, basic, concurrent, majority, election, or timeout.");
            }
        }

        Logger.log("[Main] Finished example(s): " + requestedExample);
    }

    private static void runAllExamples() throws Exception {
        runBasicReplicationExample();
        runConcurrentWorkloadExample();
        runStrictMajorityExample();
        runCoordinatorElectionExample();
        runCrashedReplicaReadExample();
    }

    // ============================================================================
    // Basic replication
    // ============================================================================

    /** A normal write through one replica, followed by a read through another. */
    private static void runBasicReplicationExample() throws Exception {
        printExampleHeader("basic replication");
        Logger.log("[Example basic] Setup: 3 replicas with replica 0 as coordinator");
        DemoSystem demo = startSystem("MainBasic", 3, COORDINATOR_ID);
        ActorRef writer = demo.client("writer", 1);
        ActorRef reader = demo.client("reader", 2);

        Logger.log("[Example basic] Action: writer sends value 101 through replica 1");
        Logger.log("[Example basic] Expected: all three replicas apply value 101");
        writer.tell(new WriteRequest(INDEX, 101), ActorRef.noSender());
        Thread.sleep(800L);

        Logger.log("[Example basic] Action: reader reads index 0 through replica 2");
        Logger.log("[Example basic] Expected: the read returns value 101");
        reader.tell(new ReadRequest(INDEX), ActorRef.noSender());
        finishSystem(demo.system, 1_000L);
        Logger.log("[Example basic] Conclusion: a value written through one replica can be read from another");
        printExampleFooter("basic replication");
    }

    // ============================================================================
    // Concurrent reads, writes, and a replica crash
    // ============================================================================

    /**
     * Two writers and two readers overlap while a non-coordinator replica crashes.
     * Both writers use the same index intentionally, so the order of their updates
     * is visible in the output and is not fixed in advance.
     */
    private static void runConcurrentWorkloadExample() throws Exception {
        printExampleHeader("concurrent workload");
        Logger.log("[Example concurrent] Setup: 5 replicas with replica 0 as coordinator");
        Logger.log("[Example concurrent] Two writers update the same index while two readers repeatedly read it");
        DemoSystem demo = startSystem("MainConcurrent", 5, COORDINATOR_ID);
        ActorRef writerA = demo.client("writerA", 1);
        ActorRef writerB = demo.client("writerB", 2);
        ActorRef readerA = demo.client("readerA", 3);
        ActorRef readerB = demo.client("readerB", 3);

        Logger.log("[Example concurrent] Action: writerA sends 10 and writerB sends 20");
        Logger.log("[Example concurrent] Expected: readers may first see the initial value 0 while writes are in flight");
        writerA.tell(new WriteRequest(INDEX, 10), ActorRef.noSender());
        writerB.tell(new WriteRequest(INDEX, 20), ActorRef.noSender());

        for (int round = 0; round < 6; round++) {
            Logger.log("[Example concurrent] Round " + round + ": both readers request index 0 from replica 3");
            readerA.tell(new ReadRequest(INDEX), ActorRef.noSender());
            readerB.tell(new ReadRequest(INDEX), ActorRef.noSender());

            if (round == 1) {
                Logger.log("[Example concurrent] Action: crash non-coordinator replica 4");
                Logger.log("[Example concurrent] Expected: the remaining replicas continue serving the workload");
                demo.replicas.get(4).tell(new Crash(Crash.Type.Now, 0), ActorRef.noSender());
            }
            if (round == 2) {
                Logger.log("[Example concurrent] Action: writerA sends 30 and writerB sends 40");
                writerA.tell(new WriteRequest(INDEX, 30), ActorRef.noSender());
                writerB.tell(new WriteRequest(INDEX, 40), ActorRef.noSender());
            }
            Thread.sleep(150L);
        }

        finishSystem(demo.system, 2_000L);
        Logger.log("[Example concurrent] Conclusion: reads overlap with writes and a replica crash; update order is observable but not predetermined");
        printExampleFooter("concurrent workload");
    }

    // ============================================================================
    // Strict-majority operation
    // ============================================================================

    /**
     * Two replicas crash, leaving exactly the strict majority needed to continue.
     */
    private static void runStrictMajorityExample() throws Exception {
        printExampleHeader("strict majority");
        Logger.log("[Example majority] Setup: 5 replicas with replica 0 as coordinator");
        DemoSystem demo = startSystem("MainMajority", 5, COORDINATOR_ID);
        Logger.log("[Example majority] Action: crash replicas 3 and 4");
        Logger.log("[Example majority] Expected: replicas 0, 1, and 2 remain, which is a strict majority");
        demo.replicas.get(3).tell(new Crash(Crash.Type.Now, 0), ActorRef.noSender());
        demo.replicas.get(4).tell(new Crash(Crash.Type.Now, 0), ActorRef.noSender());
        Thread.sleep(SETUP_DELAY_MILLIS);

        ActorRef writer = demo.client("majorityWriter", 1);
        Logger.log("[Example majority] Action: write value 909 through surviving replica 1");
        Logger.log("[Example majority] Expected: the write succeeds with exactly three replicas alive");
        writer.tell(new WriteRequest(INDEX, 909), ActorRef.noSender());
        Thread.sleep(900L);

        Logger.log("[Example majority] Action: read index 0 through each surviving replica");
        Logger.log("[Example majority] Expected: each surviving replica returns 909");
        for (int replicaId = 0; replicaId < 3; replicaId++) {
            demo.client("majorityReader" + replicaId, replicaId).tell(new ReadRequest(INDEX), ActorRef.noSender());
        }
        finishSystem(demo.system, 1_000L);
        Logger.log("[Example majority] Conclusion: the system can complete a write with a strict majority alive");
        printExampleFooter("strict majority");
    }

    // ============================================================================
    // Coordinator election
    // ============================================================================

    /**
     * The coordinator crashes before a new write, so the remaining replicas elect another one.
     */
    private static void runCoordinatorElectionExample() throws Exception {
        printExampleHeader("coordinator election");
        Logger.log("[Example election] Setup: 5 replicas with replica 0 as coordinator");
        DemoSystem demo = startSystem("MainElection", 5, COORDINATOR_ID);
        Logger.log("[Example election] Action: crash coordinator 0");
        Logger.log("[Example election] Expected: the surviving replicas elect a new coordinator");
        demo.replicas.get(COORDINATOR_ID).tell(new Crash(Crash.Type.Now, 0), ActorRef.noSender());

        Logger.log("[Example election] Observation: waiting for heartbeat detection and the ring election");
        Thread.sleep(5_000L);
        ActorRef writer = demo.client("electionWriter", 1);
        ActorRef reader = demo.client("electionReader", 2);
        Logger.log("[Example election] Action: write value 707 through replica 1 after the election");
        Logger.log("[Example election] Expected: the write is handled by the new coordinator");
        writer.tell(new WriteRequest(INDEX, 707), ActorRef.noSender());
        Thread.sleep(1_000L);
        Logger.log("[Example election] Action: read index 0 through replica 2");
        Logger.log("[Example election] Expected: the read returns 707");
        reader.tell(new ReadRequest(INDEX), ActorRef.noSender());
        finishSystem(demo.system, 1_000L);
        Logger.log("[Example election] Conclusion: the system continues after coordinator election");
        printExampleFooter("coordinator election");
    }

    // ============================================================================
    // Timeout handling
    // ============================================================================

    /** A read sent to a crashed replica should end in a client timeout. */
    private static void runCrashedReplicaReadExample() throws Exception {
        printExampleHeader("read timeout");
        Logger.log("[Example timeout] Setup: 3 replicas with replica 0 as coordinator");
        DemoSystem demo = startSystem("MainTimeout", 3, COORDINATOR_ID);
        Logger.log("[Example timeout] Action: crash replica 2");
        Logger.log("[Example timeout] Action: send a read to crashed replica 2");
        Logger.log("[Example timeout] Expected: the client reports a read timeout");
        demo.replicas.get(2).tell(new Crash(Crash.Type.Now, 0), ActorRef.noSender());
        Thread.sleep(SETUP_DELAY_MILLIS);

        ActorRef reader = demo.client("timeoutReader", 2);
        reader.tell(new ReadRequest(INDEX), ActorRef.noSender());
        finishSystem(demo.system, CLIENT_READ_TIMEOUT + 500L);
        Logger.log("[Example timeout] Conclusion: a client does not wait forever for a crashed replica");
        printExampleFooter("read timeout");
    }

    private static void printExampleHeader(String title) {
        System.out.println();
        System.out.println("===============================================================================");
        System.out.println("EXAMPLE: " + title);
        System.out.println("===============================================================================");
        System.out.println();
    }

    private static void printExampleFooter(String title) {
        System.out.println();
        System.out.println("-------------------------------------------------------------------------------");
        System.out.println("END OF EXAMPLE: " + title);
        System.out.println("-------------------------------------------------------------------------------");
        System.out.println();
    }

    private static DemoSystem startSystem(String name, int replicaCount, int coordinatorId)
            throws InterruptedException {
        ActorSystem system = ActorSystem.create(name);
        ActorRef listener = system.actorOf(DemoListener.props(name), "listener");
        Map<Integer, ActorRef> replicas = new LinkedHashMap<>();

        for (int replicaId = 0; replicaId < replicaCount; replicaId++) {
            replicas.put(
                    replicaId,
                    system.actorOf(
                            Replica.propsWithListener(
                                    replicaId,
                                    AbstractReplica.MIN_LATENCY,
                                    AbstractReplica.MAX_LATENCY,
                                    AbstractReplica.COORDINATOR_BEAT_INTERVAL,
                                    listener),
                            "Replica_" + replicaId));
        }

        InitSystem init = new InitSystem(replicas, coordinatorId);
        for (ActorRef replica : replicas.values()) {
            replica.tell(init, ActorRef.noSender());
        }
        Thread.sleep(SETUP_DELAY_MILLIS);
        return new DemoSystem(system, replicas, listener);
    }

    private static void finishSystem(ActorSystem system, long observationDelayMillis) throws Exception {
        Thread.sleep(observationDelayMillis);
        system.terminate();
        system.getWhenTerminated().toCompletableFuture().get(5, TimeUnit.SECONDS);
    }

    private static final class DemoSystem {
        private final ActorSystem system;
        private final Map<Integer, ActorRef> replicas;
        private final ActorRef listener;

        private DemoSystem(ActorSystem system, Map<Integer, ActorRef> replicas, ActorRef listener) {
            this.system = system;
            this.replicas = replicas;
            this.listener = listener;
        }

        private ActorRef client(String name, int targetReplicaId) {
            return system.actorOf(
                    Client.propsWithListener(
                            CLIENT_READ_TIMEOUT,
                            CLIENT_WRITE_TIMEOUT,
                            Optional.of(replicas.get(targetReplicaId)),
                            listener),
                    name);
        }
    }

    private static final class DemoListener extends AbstractActor {
        private final String exampleName;

        private DemoListener(String exampleName) {
            this.exampleName = exampleName;
        }

        private static Props props(String exampleName) {
            return Props.create(DemoListener.class, () -> new DemoListener(exampleName));
        }

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(
                            ReadResult.class,
                            result -> log("Observed read from replica " + result.fromReplica + " returned index "
                                    + result.index + " = " + result.value + " (success=" + result.success + ")"))
                    .match(
                            WriteResult.class,
                            result -> log("Observed write through replica " + result.fromReplica + " completed for index "
                                    + result.index + " = " + result.value + " (success=" + result.success + ")"))
                    .match(
                            ReadTimeout.class,
                            timeout -> log("Observed read timeout for replica " + timeout.replica.path().name()
                                    + " at index " + timeout.index))
                    .match(
                            WriteTimeout.class,
                            timeout -> log("Observed write timeout for replica " + timeout.replica.path().name()
                                    + " at index " + timeout.index + " = " + timeout.value))
                    .match(
                            UpdateApplied.class,
                            update -> log("Observed replica " + update.replicaId + " stored index " + update.index
                                    + " = " + update.value))
                    .match(
                            ElectionStarted.class,
                            started -> log("Observed replica " + started.replicaId
                                    + " started an election because coordinator " + started.crashedCoordinatorId
                                    + " failed"))
                    .match(
                            CoordinatorElected.class,
                            elected -> log("Observed replica " + elected.replicaId + " accepted replica "
                                    + elected.newCoordinatorId + " as the new coordinator"))
                    .match(Crash.class, crash -> log("Observed crash of " + getSender().path().name()
                            + " (mode=" + crash.type + ")"))
                    .build();
        }

        private void log(String message) {
            Logger.log("[Example " + exampleName + "] " + message);
        }
    }
}
