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
import it.unitn.ds.AbstractReplica.UpdateApplied;
import it.unitn.ds.Client;
import it.unitn.ds.TestsCommons;
import it.unitn.ds.TestsCommons.TestsSystemWrapper;
import java.time.Duration;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Regression coverage for per-client sequential consistency and replica convergence. */
class TestSequentialConsistency {

    private static final int NODE_COUNT = 3;
    private static final int COORDINATOR_ID = 0;
    private static final int WRITER_TARGET_ID = 1;
    private static final int INDEX = TestsCommons.TEST_INDEX;
    private static final int FIRST_VALUE = 101;
    private static final int SECOND_VALUE = 202;
    private static final int HEARTBEAT_INTERVAL = 1000;

    private TestsSystemWrapper system;
    private ActorRef writer;
    private TestKit writerProbe;

    @BeforeEach
    void setUp() {
        system = TestsCommons.createTestSystem(
                "sequentialConsistency",
                NODE_COUNT,
                COORDINATOR_ID,
                AbstractReplica.MIN_LATENCY,
                AbstractReplica.MAX_LATENCY,
                HEARTBEAT_INTERVAL);
        writerProbe = new TestKit(system.system);
        writer = system.system.actorOf(
                Client.propsWithListener(
                        system.client_read_timeout,
                        system.client_write_timeout,
                        Optional.of(system.actors.get(WRITER_TARGET_ID)),
                        writerProbe.getRef()),
                "writer");
    }

    @AfterEach
    void tearDown() {
        system.system.terminate();
    }

    @Test
    void oneClientObservesItsWritesAndReadsInProgramOrder() {
        sendWrite(FIRST_VALUE);
        sendRead(writer, writerProbe, WRITER_TARGET_ID);
        sendWrite(SECOND_VALUE);
        sendRead(writer, writerProbe, WRITER_TARGET_ID);

        assertWriteResult(FIRST_VALUE);
        assertReadResult(writerProbe, FIRST_VALUE, WRITER_TARGET_ID);
        assertWriteResult(SECOND_VALUE);
        assertReadResult(writerProbe, SECOND_VALUE, WRITER_TARGET_ID);

        // A completed write must eventually be visible on every correct replica.
        for (int replicaId = 0; replicaId < NODE_COUNT; replicaId++) {
            expectApplied(replicaId, FIRST_VALUE);
            expectApplied(replicaId, SECOND_VALUE);
        }

        // Reads from all replicas must agree after the ordered updates have converged.
        for (int replicaId = 0; replicaId < NODE_COUNT; replicaId++) {
            TestKit readerProbe = new TestKit(system.system);
            ActorRef reader = createClient("reader" + replicaId, readerProbe, replicaId);
            sendRead(reader, readerProbe, replicaId);
            assertReadResult(readerProbe, SECOND_VALUE, replicaId);
        }
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

    private void sendWrite(int value) {
        writer.tell(new WriteRequest(INDEX, value, null), writerProbe.getRef());
    }

    private void sendRead(ActorRef client, TestKit probe, int targetId) {
        client.tell(new ReadRequest(INDEX, system.actors.get(targetId)), probe.getRef());
    }

    private void assertWriteResult(int expectedValue) {
        WriteResult result = writerProbe.expectMsgClass(writeTimeout(), WriteResult.class);
        assertTrue(result.success, "The ordered write should complete successfully");
        assertEquals(INDEX, result.index, "The write should identify the requested index");
        assertEquals(expectedValue, result.value, "The write result should preserve the requested value");
        assertEquals(WRITER_TARGET_ID, result.fromReplica, "The write should complete through the target replica");
    }

    private void assertReadResult(TestKit probe, int expectedValue, int targetId) {
        ReadResult result = probe.expectMsgClass(readTimeout(), ReadResult.class);
        assertTrue(result.success, "The read should complete successfully");
        assertEquals(INDEX, result.index, "The read should identify the requested index");
        assertEquals(expectedValue, result.value, "The read should observe the ordered state");
        assertEquals(targetId, result.fromReplica, "The read should come from its requested replica");
    }

    private void expectApplied(int replicaId, int expectedValue) {
        system.probes
                .get(replicaId)
                .fishForSpecificMessage(
                        writeTimeout(),
                        "wait for replica " + replicaId + " to apply " + expectedValue,
                        message -> message instanceof UpdateApplied update
                                        && update.index == INDEX
                                        && update.value == expectedValue
                                ? update
                                : null);
    }

    private Duration writeTimeout() {
        return Duration.ofMillis(system.client_write_timeout);
    }

    private Duration readTimeout() {
        return Duration.ofMillis(system.client_read_timeout);
    }
}
