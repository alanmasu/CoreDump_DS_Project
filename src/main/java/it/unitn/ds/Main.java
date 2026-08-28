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

    /** A normal write through one replica, followed by a read through another. */
    private static void runBasicReplicationExample() throws Exception {
        Logger.log("[Example basic] Writing through one replica, then reading through another");
        DemoSystem demo = startSystem("MainBasic", 3, 0);
        ActorRef writer = demo.client("writer", 1);
        ActorRef reader = demo.client("reader", 2);

        writer.tell(new WriteRequest(INDEX, 101), ActorRef.noSender());
        Thread.sleep(800L);
        reader.tell(new ReadRequest(INDEX), ActorRef.noSender());
        finishSystem(demo.system, 1_000L);
    }

    /**
     * Two writers and two readers stay active while a non-coordinator replica crashes.
     */
    private static void runConcurrentWorkloadExample() throws Exception {
        Logger.log("[Example concurrent] Two writers and two readers run together");
        DemoSystem demo = startSystem("MainConcurrent", 5, 0);
        ActorRef writerA = demo.client("writerA", 1);
        ActorRef writerB = demo.client("writerB", 2);
        ActorRef readerA = demo.client("readerA", 3);
        ActorRef readerB = demo.client("readerB", 3);

        writerA.tell(new WriteRequest(INDEX, 10), ActorRef.noSender());
        writerB.tell(new WriteRequest(INDEX, 20), ActorRef.noSender());

        for (int round = 0; round < 6; round++) {
            readerA.tell(new ReadRequest(INDEX), ActorRef.noSender());
            readerB.tell(new ReadRequest(INDEX), ActorRef.noSender());

            if (round == 1) {
                Logger.log("[Example concurrent] Crashing non-coordinator replica 4");
                demo.replicas.get(4).tell(new Crash(Crash.Type.Now, 0), ActorRef.noSender());
            }
            if (round == 2) {
                writerA.tell(new WriteRequest(INDEX, 30), ActorRef.noSender());
                writerB.tell(new WriteRequest(INDEX, 40), ActorRef.noSender());
            }
            Thread.sleep(150L);
        }

        finishSystem(demo.system, 2_000L);
    }

    /** Two replicas crash, leaving exactly the strict majority needed to continue. */
    private static void runStrictMajorityExample() throws Exception {
        Logger.log("[Example majority] Replicas 3 and 4 crash; replicas 0, 1, and 2 continue");
        DemoSystem demo = startSystem("MainMajority", 5, 0);
        demo.replicas.get(3).tell(new Crash(Crash.Type.Now, 0), ActorRef.noSender());
        demo.replicas.get(4).tell(new Crash(Crash.Type.Now, 0), ActorRef.noSender());
        Thread.sleep(SETUP_DELAY_MILLIS);

        ActorRef writer = demo.client("majorityWriter", 1);
        writer.tell(new WriteRequest(INDEX, 909), ActorRef.noSender());
        Thread.sleep(900L);

        for (int replicaId = 0; replicaId < 3; replicaId++) {
            demo.client("majorityReader" + replicaId, replicaId).tell(new ReadRequest(INDEX), ActorRef.noSender());
        }
        finishSystem(demo.system, 1_000L);
    }

    /** The coordinator crashes before a new write, so the remaining replicas elect another one. */
    private static void runCoordinatorElectionExample() throws Exception {
        Logger.log("[Example election] Coordinator 0 crashes before the next write");
        DemoSystem demo = startSystem("MainElection", 5, 0);
        demo.replicas.get(0).tell(new Crash(Crash.Type.Now, 0), ActorRef.noSender());

        // Give heartbeat detection and the ring election time to finish.
        Thread.sleep(5_000L);
        ActorRef writer = demo.client("electionWriter", 1);
        ActorRef reader = demo.client("electionReader", 2);
        writer.tell(new WriteRequest(INDEX, 707), ActorRef.noSender());
        Thread.sleep(1_000L);
        reader.tell(new ReadRequest(INDEX), ActorRef.noSender());
        finishSystem(demo.system, 1_000L);
    }

    /** A read sent to a crashed replica should end in a client timeout. */
    private static void runCrashedReplicaReadExample() throws Exception {
        Logger.log("[Example timeout] A read sent to crashed replica 2 should time out");
        DemoSystem demo = startSystem("MainTimeout", 3, 0);
        demo.replicas.get(2).tell(new Crash(Crash.Type.Now, 0), ActorRef.noSender());
        Thread.sleep(SETUP_DELAY_MILLIS);

        ActorRef reader = demo.client("timeoutReader", 2);
        reader.tell(new ReadRequest(INDEX), ActorRef.noSender());
        finishSystem(demo.system, CLIENT_READ_TIMEOUT + 500L);
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
                    .match(ReadResult.class, result -> log("READ result " + result))
                    .match(WriteResult.class, result -> log("WRITE result " + result))
                    .match(
                            ReadTimeout.class,
                            timeout -> log("READ timeout for replica "
                                    + timeout.replica.path().name()))
                    .match(
                            WriteTimeout.class,
                            timeout -> log("WRITE timeout for replica "
                                    + timeout.replica.path().name()))
                    .match(UpdateApplied.class, update -> log("UPDATE APPLIED " + update))
                    .match(ElectionStarted.class, started -> log("ELECTION STARTED " + started))
                    .match(CoordinatorElected.class, elected -> log("COORDINATOR ELECTED " + elected))
                    .match(Crash.class, crash -> log("CRASH requested: " + crash.type))
                    .build();
        }

        private void log(String message) {
            Logger.log("[Example " + exampleName + "] " + message);
        }
    }
}
