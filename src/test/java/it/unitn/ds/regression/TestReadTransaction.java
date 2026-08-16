package it.unitn.ds.regression;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;
import java.time.Duration;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.javadsl.TestKit;
import it.unitn.ds.AbstractClient.ReadRequest;
import it.unitn.ds.AbstractClient.ReadResult;
import it.unitn.ds.Client;
import it.unitn.ds.Logger;
import it.unitn.ds.Replica;

class TestReadTransaction {

    @BeforeAll
    static void setup() {
        Logger.setDestinationStdout();
        Logger.setDebugEnabled(true);
        Logger.setLoggingEnabled(true);
    }

    @Test
    void testReadTransaction() {
        ActorSystem sys = ActorSystem.create("TestReadTransaction");
        TestKit probe = new TestKit(sys);
        ActorRef replica = sys.actorOf(Replica.props(0, 10, 100, 1000), "replica");
        ActorRef client = sys.actorOf(Client.propsWithListener(3 * 100,   1000, Optional.empty(), probe.getRef()), "client");

        ReadRequest readRequest = new ReadRequest(0, replica);
        client.tell(readRequest, replica);

        ReadResult readResult = probe.expectMsgClass(ReadResult.class);
        
        assertTrue(readResult.success, "Read transaction should be successful");
    }


    @Test
    void testReadTransactionTimeout() {
        ActorSystem sys = ActorSystem.create("TestReadTransactionTimeout");
        TestKit probe = new TestKit(sys);
        TestKit probeReplica = new TestKit(sys);
        ActorRef client = sys.actorOf(Client.propsWithListener(1 * 100,   1000, Optional.empty(), probe.getRef()), "client");

        ReadRequest readRequest = new ReadRequest(0, probeReplica.getRef());
        client.tell(readRequest, probeReplica.getRef());

        // Expecting a timeout message since the read timeout is set to 100ms
        probe.expectMsgClass(Duration.ofMillis(205), it.unitn.ds.AbstractClient.ReadTimeout.class);
    }
    
}
