package it.unitn.ds.regression;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import akka.actor.ActorRef;
import akka.testkit.javadsl.TestKit;
import it.unitn.ds.AbstractClient.ReadRequest;
import it.unitn.ds.AbstractClient.ReadResult;
import it.unitn.ds.AbstractClient.WriteRequest;
import it.unitn.ds.AbstractClient.WriteResult;
import it.unitn.ds.AbstractReplica;
import it.unitn.ds.AbstractReplica.Crash;
import it.unitn.ds.AbstractReplica.UpdateApplied;
import it.unitn.ds.Client;
import it.unitn.ds.TestsCommons;
import it.unitn.ds.TestsCommons.TestsSystemWrapper;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Regression coverage for concurrent writer and reader client workloads. */
class TestConcurrentWriteReadWorkload {

    private static final int NODE_COUNT = 5;
    private static final int COORDINATOR_ID = 0;
    private static final int WRITER_A_TARGET_ID = 1;
    private static final int WRITER_B_TARGET_ID = 2;
    private static final int READER_TARGET_ID = 3;
    private static final int CRASHED_REPLICA_ID = 4;
    private static final int INDEX = TestsCommons.TEST_INDEX;
    private static final int HEARTBEAT_INTERVAL = 1000;
    private static final int REPEATED_READ_COUNT = 8;
    private static final int[] WRITE_VALUES = {10, 20, 30, 40};

    private TestsSystemWrapper system;
    private ActorRef writerA;
    private ActorRef writerB;
    private ActorRef readerA;
    private ActorRef readerB;
    private TestKit writerAProbe;
    private TestKit writerBProbe;
    private TestKit readerAProbe;
    private TestKit readerBProbe;

    @BeforeEach
    void setUp() {
        system = TestsCommons.createTestSystem(
                "concurrentWriteReadWorkload",
                NODE_COUNT,
                COORDINATOR_ID,
                AbstractReplica.MIN_LATENCY,
                AbstractReplica.MAX_LATENCY,
                HEARTBEAT_INTERVAL);

        writerAProbe = new TestKit(system.system);
        writerBProbe = new TestKit(system.system);
        readerAProbe = new TestKit(system.system);
        readerBProbe = new TestKit(system.system);

        writerA = createClient("writerA", writerAProbe, WRITER_A_TARGET_ID);
        writerB = createClient("writerB", writerBProbe, WRITER_B_TARGET_ID);
        readerA = createClient("readerA", readerAProbe, READER_TARGET_ID);
        readerB = createClient("readerB", readerBProbe, READER_TARGET_ID);
    }

    @AfterEach
    void tearDown() {
        system.system.terminate();
    }

    @Test
    void independentWritersAndReadersRunTogetherDuringAReplicaCrash() {
        // Each writer preserves its own request order, but the two writers execute concurrently.
        sendWrite(writerA, 10);
        sendWrite(writerA, 30);
        sendWrite(writerB, 20);
        sendWrite(writerB, 40);

        // These reads are queued on independent clients while the writes are still in flight.
        sendReads(readerA);
        sendReads(readerB);

        ActorRef crashedReplica = system.actors.get(CRASHED_REPLICA_ID);
        crashedReplica.tell(new Crash(Crash.Type.Now, 0), readerAProbe.getRef());
        awaitCrashNotification();

        assertWriteResults(writerAProbe, List.of(10, 30));
        assertWriteResults(writerBProbe, List.of(20, 40));

        List<ReadResult> readerAResults = expectReadResults(readerAProbe);
        List<ReadResult> readerBResults = expectReadResults(readerBProbe);
        List<Integer> appliedOrder = expectAppliedUpdatesOnReadTarget();

        assertWriterOrdersArePreserved(appliedOrder);
        assertReadsFollowReplicaOrder(readerAResults, appliedOrder);
        assertReadsFollowReplicaOrder(readerBResults, appliedOrder);

        // Once the read target has applied every update, both readers must observe its final value.
        sendRead(readerA);
        sendRead(readerB);
        int finalValue = appliedOrder.get(appliedOrder.size() - 1);
        assertReadResult(readerAProbe, finalValue);
        assertReadResult(readerBProbe, finalValue);
    }

    private ActorRef createClient(String name, TestKit probe, int targetId) {
        return system.system.actorOf(
                Client.propsWithListener(
                        system.client_read_timeout,
                        system.client_write_timeout,
                        Optional.of(system.actors.get(targetId)),
                        probe.getRef()),
                name);
    }

    private void sendWrite(ActorRef writer, int value) {
        writer.tell(new WriteRequest(INDEX, value, null), writer == writerA ? writerAProbe.getRef() : writerBProbe.getRef());
    }

    private void sendReads(ActorRef reader) {
        for (int i = 0; i < REPEATED_READ_COUNT; i++) {
            sendRead(reader);
        }
    }

    private void sendRead(ActorRef reader) {
        TestKit probe = reader == readerA ? readerAProbe : readerBProbe;
        reader.tell(new ReadRequest(INDEX, null), probe.getRef());
    }

    private void awaitCrashNotification() {
        system.probes
                .get(CRASHED_REPLICA_ID)
                .fishForSpecificMessage(
                        Duration.ofSeconds(1),
                        "wait for replica " + CRASHED_REPLICA_ID + " to crash",
                        message -> message instanceof Crash ? (Crash) message : null);
    }

    private void assertWriteResults(TestKit probe, List<Integer> expectedValues) {
        for (int expectedValue : expectedValues) {
            WriteResult result = probe.expectMsgClass(writeTimeout(), WriteResult.class);
            assertTrue(result.success, "Every writer operation should complete successfully");
            assertEquals(INDEX, result.index, "The write result should identify the requested index");
            assertEquals(expectedValue, result.value, "Each writer must preserve its own request order");
        }
    }

    private List<ReadResult> expectReadResults(TestKit probe) {
        List<ReadResult> results = new ArrayList<>();
        for (int i = 0; i < REPEATED_READ_COUNT; i++) {
            ReadResult result = probe.expectMsgClass(readTimeout(), ReadResult.class);
            assertTrue(result.success, "Reads against the correct replica should complete successfully");
            assertEquals(INDEX, result.index, "The read result should identify the requested index");
            assertEquals(READER_TARGET_ID, result.fromReplica, "Readers should use the same read target");
            results.add(result);
        }
        return results;
    }

    private List<Integer> expectAppliedUpdatesOnReadTarget() {
        List<Integer> appliedValues = new ArrayList<>();
        for (int i = 0; i < WRITE_VALUES.length; i++) {
            UpdateApplied update = system.probes
                    .get(READER_TARGET_ID)
                    .expectMsgClass(writeTimeout(), UpdateApplied.class);
            assertEquals(INDEX, update.index, "Every applied update should target the requested index");
            appliedValues.add(update.value);
        }
        assertEquals(
                new HashSet<>(Set.of(WRITE_VALUES[0], WRITE_VALUES[1], WRITE_VALUES[2], WRITE_VALUES[3])),
                new HashSet<>(appliedValues),
                "The read target should apply every submitted update exactly once");
        return appliedValues;
    }

    private void assertWriterOrdersArePreserved(List<Integer> appliedOrder) {
        assertTrue(
                appliedOrder.indexOf(10) < appliedOrder.indexOf(30),
                "Writer A's second update must follow its first update");
        assertTrue(
                appliedOrder.indexOf(20) < appliedOrder.indexOf(40),
                "Writer B's second update must follow its first update");
    }

    private void assertReadsFollowReplicaOrder(List<ReadResult> results, List<Integer> appliedOrder) {
        Map<Integer, Integer> order = new HashMap<>();
        for (int i = 0; i < appliedOrder.size(); i++) {
            order.put(appliedOrder.get(i), i);
        }

        int lastObservedOrder = -1;
        Set<Integer> allowedValues = new HashSet<>();
        allowedValues.add(0);
        for (int value : WRITE_VALUES) {
            allowedValues.add(value);
        }

        for (ReadResult result : results) {
            assertTrue(allowedValues.contains(result.value), "A read must return an initial or submitted value");
            int observedOrder = result.value == 0 ? -1 : order.get(result.value);
            assertTrue(
                    observedOrder >= lastObservedOrder,
                    "A reader must not observe the read target moving backwards in update order");
            lastObservedOrder = observedOrder;
        }
    }

    private void assertReadResult(TestKit probe, int expectedValue) {
        ReadResult result = probe.expectMsgClass(readTimeout(), ReadResult.class);
        assertTrue(result.success, "The final read should complete successfully");
        assertEquals(INDEX, result.index, "The final read should identify the requested index");
        assertEquals(expectedValue, result.value, "The final read should observe the converged value");
        assertEquals(READER_TARGET_ID, result.fromReplica, "The final read should use the read target");
    }

    private Duration writeTimeout() {
        return Duration.ofMillis(system.client_write_timeout);
    }

    private Duration readTimeout() {
        return Duration.ofMillis(system.client_read_timeout);
    }
}
