package it.unitn.ds.regression;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import akka.actor.ActorRef;
import akka.testkit.TestActorRef;
import akka.testkit.javadsl.TestKit;
import it.unitn.ds.AbstractClient.ReadRequest;
import it.unitn.ds.AbstractClient.ReadResult;
import it.unitn.ds.AbstractClient.ReadTimeout;
import it.unitn.ds.AbstractReplica;
import it.unitn.ds.AbstractReplica.Crash;
import it.unitn.ds.Client;
import it.unitn.ds.Logger;
import it.unitn.ds.Replica;
import it.unitn.ds.TestsCommons;
import it.unitn.ds.TestsCommons.TestsSystemWrapper;
import java.time.Duration;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class TestReadTransaction {

    static final int NODES_N = 3;
    static final int COORDINATOR_ID = 0;
    TestsSystemWrapper sys;
    TestKit replicaProbe;
    TestKit clientProbe;
    TestActorRef<Replica> replica;
    TestActorRef<Client> client;

    @AfterEach
    void teardown() {
        sys.system.terminate();
    }

    @BeforeEach
    void setup() {
        sys = TestsCommons.createTestSystem("oneClientWrite_" + COORDINATOR_ID, NODES_N, COORDINATOR_ID);
        replicaProbe = new TestKit(sys.system);
        replica = TestActorRef.create(
                sys.system,
                Replica.propsWithListener(
                        NODES_N + 1,
                        AbstractReplica.MIN_LATENCY,
                        AbstractReplica.MAX_LATENCY,
                        TestsCommons.TEST_COORDINATOR_BEAT_INTERVAL,
                        replicaProbe.getRef()),
                String.format("%d", NODES_N + 1));
        clientProbe = new TestKit(sys.system);
        client = TestActorRef.create(
                sys.system,
                Client.propsWithListener(
                        sys.client_read_timeout,
                        sys.client_write_timeout,
                        Optional.ofNullable(sys.actors.get(0)),
                        clientProbe.getRef()),
                "client1");
        Logger.setDestinationStdout();
        Logger.setDebugEnabled(true);
        Logger.setLoggingEnabled(true);
    }

    @Test
    void testReadTransaction() {
        ReadRequest readRequest = new ReadRequest(0, replica);
        replica.underlyingActor().setPosition(0, 35);

        // Sending the read request to the client
        client.tell(readRequest, replica);

        ReadResult readResult = clientProbe.expectMsgClass(ReadResult.class);

        assertTrue(readResult.success, "Read transaction should be successful");
        assertEquals(0, readResult.index, "Read transaction should return the correct index");
        assertEquals(35, readResult.value, "Read transaction should return the correct value");
    }

    @Test
    void testReadTransactionTimeout() {
        ReadRequest readRequest = new ReadRequest(0, replica);
        Crash replicaCrash = new Crash(Crash.Type.Now, 0);
        replica.tell(replicaCrash, ActorRef.noSender());
        replicaProbe.expectMsgClass(Crash.class);
        client.tell(readRequest, ActorRef.noSender());

        // Expecting a timeout message since the read timeout is set to 100ms
        ReadTimeout readTimeout = clientProbe.expectMsgClass(
                Duration.ofMillis(TestsCommons.getClientReadTimeout(AbstractReplica.MAX_LATENCY, NODES_N + 1)),
                ReadTimeout.class);
        assertEquals(0, readTimeout.index, "Read transaction should timeout for index 0");
        assertEquals(replica, readTimeout.replica, "Read transaction should timeout for the correct replica");
        assertEquals(client, readTimeout.client, "Read transaction should timeout for the correct client");
    }
}
