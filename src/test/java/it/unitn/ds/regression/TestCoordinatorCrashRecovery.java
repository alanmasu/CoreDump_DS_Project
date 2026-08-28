package it.unitn.ds.regression;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import akka.actor.ActorRef;
import akka.testkit.javadsl.TestKit;
import it.unitn.ds.AbstractClient.ReadRequest;
import it.unitn.ds.AbstractClient.ReadResult;
import it.unitn.ds.AbstractClient.WriteRequest;
import it.unitn.ds.AbstractClient.WriteResult;
import it.unitn.ds.AbstractReplica;
import it.unitn.ds.AbstractReplica.CoordinatorElected;
import it.unitn.ds.AbstractReplica.Crash;
import it.unitn.ds.AbstractReplica.UpdateApplied;
import it.unitn.ds.Client;
import it.unitn.ds.TestsCommons;
import it.unitn.ds.TestsCommons.TestsSystemWrapper;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Regression coverage for recovering an update after a coordinator crash during broadcast. */
class TestCoordinatorCrashRecovery {

    private static final int NODE_COUNT = 5;
    private static final int COORDINATOR_ID = 0;
    private static final int WRITER_TARGET_ID = 1;
    private static final int READER_TARGET_ID = 2;
    private static final int INDEX = TestsCommons.TEST_INDEX;
    private static final int INTERRUPTED_VALUE = 505;
    private static final int FOLLOW_UP_VALUE = 606;
    private static final int HEARTBEAT_INTERVAL = 1000;

    private TestsSystemWrapper system;
    private ActorRef writer;
    private TestKit writerProbe;

    @BeforeEach
    void setUp() {
        system = TestsCommons.createTestSystem(
                "coordinatorCrashRecovery",
                NODE_COUNT,
                COORDINATOR_ID,
                AbstractReplica.MIN_LATENCY,
                AbstractReplica.MAX_LATENCY,
                HEARTBEAT_INTERVAL);
        writerProbe = new TestKit(system.system);
        writer = createClient("writer", writerProbe, WRITER_TARGET_ID);
    }

    @AfterEach
    void tearDown() {
        system.system.terminate();
    }

    @Test
    void interruptedUpdateIsRecoveredBeforeTheNextWrite() {
        ActorRef coordinator = system.actors.get(COORDINATOR_ID);
        coordinator.tell(new Crash(Crash.Type.Update, 1), writerProbe.getRef());
        system.probes.get(COORDINATOR_ID).expectMsgClass(Crash.class);

        // Queue the follow-up write immediately: the client must not start it until recovery completes.
        sendWrite(INTERRUPTED_VALUE);
        sendWrite(FOLLOW_UP_VALUE);

        assertWriteResult(INTERRUPTED_VALUE);
        assertWriteResult(FOLLOW_UP_VALUE);

        List<Integer> electedCoordinatorIds = new ArrayList<>();
        for (int replicaId = 1; replicaId < NODE_COUNT; replicaId++) {
            CoordinatorElected elected = expectCoordinatorElected(replicaId);
            assertNotEquals(COORDINATOR_ID, elected.newCoordinatorId, "The crashed coordinator cannot be re-elected");
            electedCoordinatorIds.add(elected.newCoordinatorId);
        }
        assertTrue(
                electedCoordinatorIds.stream().allMatch(electedCoordinatorIds.get(0)::equals),
                "All surviving replicas should agree on the elected coordinator");

        // The recovered first update and the follow-up update must reach every survivor.
        for (int replicaId = 1; replicaId < NODE_COUNT; replicaId++) {
            expectApplied(replicaId, INTERRUPTED_VALUE);
            expectApplied(replicaId, FOLLOW_UP_VALUE);
        }

        TestKit readerProbe = new TestKit(system.system);
        ActorRef reader = createClient("reader", readerProbe, READER_TARGET_ID);
        reader.tell(new ReadRequest(INDEX, system.actors.get(READER_TARGET_ID)), readerProbe.getRef());
        ReadResult result = readerProbe.expectMsgClass(readTimeout(), ReadResult.class);
        assertTrue(result.success, "A read should succeed after coordinator recovery");
        assertEquals(FOLLOW_UP_VALUE, result.value, "The read should observe the post-recovery write");
        assertEquals(READER_TARGET_ID, result.fromReplica, "The read should come from the requested replica");
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

    private void assertWriteResult(int expectedValue) {
        WriteResult result = writerProbe.expectMsgClass(writeTimeout(), WriteResult.class);
        assertTrue(result.success, "The write should complete after coordinator recovery");
        assertEquals(INDEX, result.index, "The write should identify the requested index");
        assertEquals(expectedValue, result.value, "The write result should preserve the requested value");
        assertEquals(WRITER_TARGET_ID, result.fromReplica, "The write should complete through its target replica");
    }

    private CoordinatorElected expectCoordinatorElected(int replicaId) {
        return system.probes
                .get(replicaId)
                .fishForSpecificMessage(
                        writeTimeout(),
                        "wait for replica " + replicaId + " to observe a new coordinator",
                        message -> message instanceof CoordinatorElected elected
                                        && elected.newCoordinatorId != COORDINATOR_ID
                                ? elected
                                : null);
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
