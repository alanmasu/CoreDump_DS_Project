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
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Regression coverage for progress with exactly a strict majority of replicas alive. */
class TestMajorityBoundary {

    private static final int NODE_COUNT = 5;
    private static final int COORDINATOR_ID = 0;
    private static final int WRITER_TARGET_ID = 1;
    private static final int FIRST_CRASHED_REPLICA_ID = 3;
    private static final int SECOND_CRASHED_REPLICA_ID = 4;
    private static final int INDEX = TestsCommons.TEST_INDEX;
    private static final int VALUE = 909;
    private static final int HEARTBEAT_INTERVAL = 1000;

    private TestsSystemWrapper system;
    private ActorRef writer;
    private TestKit writerProbe;

    @BeforeEach
    void setUp() {
        system = TestsCommons.createTestSystem(
                "majorityBoundary",
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
    void writeAndReadsSucceedWithExactlyThreeOfFiveReplicasAlive() {
        crashReplica(FIRST_CRASHED_REPLICA_ID);
        crashReplica(SECOND_CRASHED_REPLICA_ID);

        writer.tell(new WriteRequest(INDEX, VALUE, null), writerProbe.getRef());
        WriteResult writeResult = writerProbe.expectMsgClass(writeTimeout(), WriteResult.class);
        assertTrue(writeResult.success, "A strict majority should be sufficient for a write");
        assertEquals(INDEX, writeResult.index, "The write should identify the requested index");
        assertEquals(VALUE, writeResult.value, "The write result should preserve the requested value");
        assertEquals(WRITER_TARGET_ID, writeResult.fromReplica, "The write should use its correct target replica");

        for (int replicaId = 0; replicaId <= WRITER_TARGET_ID + 1; replicaId++) {
            expectApplied(replicaId);
        }

        for (int replicaId = 0; replicaId <= WRITER_TARGET_ID + 1; replicaId++) {
            TestKit readerProbe = new TestKit(system.system);
            ActorRef reader = createClient("reader" + replicaId, readerProbe, replicaId);
            reader.tell(new ReadRequest(INDEX, system.actors.get(replicaId)), readerProbe.getRef());
            ReadResult readResult = readerProbe.expectMsgClass(readTimeout(), ReadResult.class);
            assertTrue(readResult.success, "A surviving replica should serve reads");
            assertEquals(VALUE, readResult.value, "Every surviving replica should expose the committed value");
            assertEquals(replicaId, readResult.fromReplica, "The read should come from its requested replica");
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

    private void crashReplica(int replicaId) {
        system.actors.get(replicaId).tell(new Crash(Crash.Type.Now, 0), writerProbe.getRef());
        system.probes.get(replicaId).expectMsgClass(Crash.class);
    }

    private void expectApplied(int replicaId) {
        system.probes
                .get(replicaId)
                .fishForSpecificMessage(
                        writeTimeout(),
                        "wait for replica " + replicaId + " to apply " + VALUE,
                        message -> message instanceof UpdateApplied update
                                        && update.index == INDEX
                                        && update.value == VALUE
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
