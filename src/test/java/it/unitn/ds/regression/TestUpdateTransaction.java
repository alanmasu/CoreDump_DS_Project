package it.unitn.ds.regression;

import static org.junit.jupiter.api.Assertions.assertEquals;

import akka.actor.ActorRef;
import akka.testkit.javadsl.TestKit;
import it.unitn.ds.AbstractClient.WriteRequest;
import it.unitn.ds.AbstractClient.WriteResult;
import it.unitn.ds.AbstractClient.WriteTimeout;
import it.unitn.ds.AbstractReplica;
import it.unitn.ds.AbstractReplica.Crash;
import it.unitn.ds.Client;
import it.unitn.ds.Logger;
import it.unitn.ds.TestsCommons;
import it.unitn.ds.TestsCommons.TestsSystemWrapper;
import java.time.Duration;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.CsvSource;

@ParameterizedClass
@CsvSource({
    "0,3", "0,5", "1,3", "1,5",
})
class TestUpdateTransaction {

    @Parameter(0)
    int coordinatorId = 0;

    @Parameter(1)
    int nNodes;

    static final Crash CRASH_NOW = new Crash(Crash.Type.Now, 0);
    static final int HEARTBEAT_INTERVAL = 150;
    TestsSystemWrapper sys;
    TestKit replicaProbe;
    TestKit clientProbe;
    ActorRef replica;
    ActorRef client;

    @AfterEach
    void teardown() {
        sys.system.terminate();
    }

    @BeforeEach
    void setup() {
        int replicaId = 1;
        sys = TestsCommons.createTestSystem(
                "oneClientWrite_" + coordinatorId,
                nNodes,
                coordinatorId,
                AbstractReplica.MIN_LATENCY,
                AbstractReplica.MAX_LATENCY,
                HEARTBEAT_INTERVAL);
        replicaProbe = sys.probes.get(replicaId);
        replica = sys.actors.get(replicaId);
        clientProbe = new TestKit(sys.system);
        client = sys.system.actorOf(
                Client.propsWithListener(
                        sys.client_read_timeout, sys.client_write_timeout, Optional.empty(), clientProbe.getRef()),
                "0");
        Logger.setDestinationStdout();
        Logger.setDebugEnabled(true);
        Logger.setLoggingEnabled(true);
    }

    void startWriteTransaction(int index, int value) {
        WriteRequest writeRequest = new WriteRequest(index, value, replica);
        client.tell(writeRequest, ActorRef.noSender());
    }

    private void assertWriteTimedOut() {
        startWriteTransaction(0, 42);
        clientProbe.expectNoMessage(getClientWriteTimeout());
        WriteTimeout writeTimeout = clientProbe.expectMsgClass(WriteTimeout.class);
        assertEquals(0, writeTimeout.index, "The index in the WriteTimeout should match the requested index.");
        assertEquals(42, writeTimeout.value, "The value in the WriteTimeout should match the requested value.");
        assertEquals(client, writeTimeout.client, "The client in the WriteTimeout should match the requesting client.");
        assertEquals(replica, writeTimeout.replica, "The replica in the WriteTimeout should match the target replica.");
    }

    Duration getClientWriteTimeout() {
        return Duration.ofMillis(TestsCommons.getClientWriteTimeout(AbstractReplica.MAX_LATENCY, sys.getNNodes()));
    }

    @Test
    void testUpdateTransaction() {
        startWriteTransaction(0, 42);
        WriteResult writeResult = clientProbe.expectMsgClass(getClientWriteTimeout(), WriteResult.class);
        assertEquals(0, writeResult.index, "The index in the WriteResult should match the requested index.");
        assertEquals(42, writeResult.value, "The value in the WriteResult should match the requested value.");
        assertEquals(true, writeResult.success, "The WriteResult should indicate a successful write.");
    }

    @Test
    void testUpdateReplicaCrash() {
        replica.tell(CRASH_NOW, ActorRef.noSender());
        replicaProbe.expectMsgClass(Crash.class);
        assertWriteTimedOut();
    }

    @Test
    void testUpdateCoordinatorCrash() {
        sys.actors.get(coordinatorId).tell(CRASH_NOW, ActorRef.noSender());
        sys.probes.get(coordinatorId).expectMsgClass(Crash.class);
        if (replica.equals(sys.actors.get(coordinatorId))) {
            // The contacted replica itself is crashed, so it cannot accept the request.
            assertWriteTimedOut();
            return;
        }

        // A live follower can retain the request, elect a coordinator, and safely
        // retry because the failed coordinator never sent UPDATE for this write.
        startWriteTransaction(0, 42);
        WriteResult writeResult = clientProbe.expectMsgClass(getClientWriteTimeout(), WriteResult.class);
        assertEquals(0, writeResult.index);
        assertEquals(42, writeResult.value);
        assertEquals(true, writeResult.success);
    }
}
