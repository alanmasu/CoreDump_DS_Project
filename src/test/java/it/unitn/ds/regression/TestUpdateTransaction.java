package it.unitn.ds.regression;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Optional;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import akka.actor.ActorRef;
// import akka.actor.ActorSystem;
// import akka.testkit.TestActorRef;
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


class TestUpdateTransaction {
    
    final static int NODES_N = 3;
    final static int COORDINATOR_ID = 0;
    TestsSystemWrapper sys;
    TestKit replicaProbe;
    TestKit clientProbe;
    // TestActorRef<Replica> replica;
    // TestActorRef<Client> client;
    ActorRef replica;
    ActorRef client;

    @AfterEach
    void teardown() {
        sys.system.terminate();
    }

    @BeforeEach
    void setup() {
        // replica = TestActorRef.create(sys.system, Replica.propsWithListener(NODES_N + 1, COORDINATOR_ID, sys.min_latency, sys.max_latency, replicaProbe.getRef()),
        //                                         "replica1");
        // client = TestActorRef.create(sys.system,  Client.propsWithListener(sys.client_read_timeout, 
        //                                                                  sys.client_write_timeout,
        //                                                                  Optional.empty(),
        //                                                                  clientProbe.getRef()),
        //                                         "client1");
        int replicaId = 1;
        sys = TestsCommons.createTestSystem("oneClientWrite_" + COORDINATOR_ID, NODES_N, COORDINATOR_ID);
        replicaProbe = sys.probes.get(replicaId);
        clientProbe = new TestKit(sys.system);
        replica = sys.actors.get(replicaId);
        client = sys.system.actorOf(   Client.propsWithListener(sys.client_read_timeout, 
                                                                         sys.client_write_timeout,
                                                                         Optional.empty(),
                                                                         clientProbe.getRef()),
                                                "0");
        Logger.setDestinationStdout();
        Logger.setDebugEnabled(true);
        Logger.setLoggingEnabled(true);
        System.out.println("getClientWriteTimeout: " + TestsCommons.getClientWriteTimeout(AbstractReplica.MAX_LATENCY, sys.getNNodes()));
    }

    @Test
    void testUpdateTransaction() {
        WriteRequest writeRequest = new WriteRequest(0, 42, replica);
        client.tell(writeRequest, ActorRef.noSender());

        WriteResult writeResult = clientProbe.expectMsgClass(Duration.ofMillis(
                                                            (TestsCommons.getClientWriteTimeout(AbstractReplica.MAX_LATENCY, sys.getNNodes())) - 100),
                                                             WriteResult.class);
        assertEquals(0, writeResult.index, "The index in the WriteResult should match the requested index.");
        assertEquals(42, writeResult.value, "The value in the WriteResult should match the requested value.");
        assertEquals(true, writeResult.success, "The WriteResult should indicate a successful write.");
    }
    
    @Test
    void testUpdateReplicaCrash() {
        Crash replicaCrash = new Crash(Crash.Type.Now, 0);
        replica.tell(replicaCrash, ActorRef.noSender());
        
        WriteRequest writeRequest = new WriteRequest(0, 42, replica);
        client.tell(writeRequest, ActorRef.noSender());
        clientProbe.expectNoMessage(Duration.ofMillis(TestsCommons.getClientWriteTimeout(AbstractReplica.MAX_LATENCY, sys.getNNodes())));
        WriteTimeout writeTimeout = clientProbe.expectMsgClass(WriteTimeout.class);
        assertEquals(0, writeTimeout.index, "The index in the WriteTimeout should match the requested index.");
        assertEquals(42, writeTimeout.value, "The value in the WriteTimeout should match the requested value.");
        assertEquals(client, writeTimeout.client, "The client in the WriteTimeout should match the requesting client.");
        assertEquals(replica, writeTimeout.replica, "The replica in the WriteTimeout should match the target replica.");
    }

    @Test
    void testUpdateCoordinatorCrash() {
        Crash coordinatorCrash = new Crash(Crash.Type.Now, 0);
        sys.actors.get(COORDINATOR_ID).tell(coordinatorCrash, ActorRef.noSender());
        
        WriteRequest writeRequest = new WriteRequest(0, 42, replica);
        client.tell(writeRequest, ActorRef.noSender());
        clientProbe.expectNoMessage(Duration.ofMillis(TestsCommons.getClientWriteTimeout(AbstractReplica.MAX_LATENCY, sys.getNNodes())));
        WriteTimeout writeTimeout = clientProbe.expectMsgClass(WriteTimeout.class);
        assertEquals(0, writeTimeout.index, "The index in the WriteTimeout should match the requested index.");
        assertEquals(42, writeTimeout.value, "The value in the WriteTimeout should match the requested value.");
        assertEquals(client, writeTimeout.client, "The client in the WriteTimeout should match the requesting client.");
        assertEquals(replica, writeTimeout.replica, "The replica in the WriteTimeout should match the target replica.");
    }


}
