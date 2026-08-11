package it.unitn.ds.regression;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.javadsl.TestKit;
import it.unitn.ds.EpochPair;
import it.unitn.ds.Logger;
import it.unitn.ds.Replica;
import it.unitn.ds.TestTransaction.TestMsg;
import it.unitn.ds.Transaction.TransactionId;

public class TestDispatcher {

    @BeforeAll
    static void setup() {
        Logger.setDestinationStdout();
        Logger.setDebugEnabled(true);
        Logger.setLoggingEnabled(true);
    }
    
    @Test
    public void testReplicaDispatcher() {
        ActorSystem sys = ActorSystem.create("TestDispatcher");
		TestKit probe = new TestKit(sys);
        ActorRef replica = sys.actorOf(Replica.propsWithListener(0, 1000, 1000, 1000, probe.getRef()), "replica");

        EpochPair epochPair = new EpochPair(0, 0);
        TransactionId transactionId = new TransactionId(replica, 1);
        
        TestMsg startMsg = new TestMsg(transactionId, epochPair, probe.getRef(), "start");
        TestMsg ackMsg = new TestMsg(transactionId, epochPair, probe.getRef(), "ack");
        replica.tell(startMsg, probe.getRef());
        
        TestMsg receivedMsg = probe.expectMsgClass(TestMsg.class);
        assertEquals(ackMsg.content, receivedMsg.content, "The received message should be an ack");
        replica.tell(ackMsg, probe.getRef());
        
    
        TestMsg doneMsg = new TestMsg(transactionId, epochPair, probe.getRef(), "done");
        receivedMsg = probe.expectMsgClass(TestMsg.class);
        assertEquals(doneMsg.content, receivedMsg.content, "The received message should match the sent message.");
        replica.tell(doneMsg, probe.getRef());

        sys.terminate();
    }

    @Test
    void testClientDispatcher() {
        ActorSystem sys = ActorSystem.create("TestDispatcher");
        TestKit probe = new TestKit(sys);
        ActorRef client = sys.actorOf(it.unitn.ds.Client.propsWithListener(1000, 1000, null, probe.getRef()), "client");

        EpochPair epochPair = new EpochPair(0, 0);
        TransactionId transactionId = new TransactionId(client, 1);

        TestMsg startMsg = new TestMsg(transactionId, epochPair, probe.getRef(), "start");
        TestMsg ackMsg = new TestMsg(transactionId, epochPair, probe.getRef(), "ack");
        client.tell(startMsg, probe.getRef());

        TestMsg receivedMsg = probe.expectMsgClass(TestMsg.class);
        assertEquals(receivedMsg.content, ackMsg.content, "The received message should be an ack");
        client.tell(ackMsg, probe.getRef());

        TestMsg doneMsg = new TestMsg(transactionId , epochPair, probe.getRef(), "done");
        receivedMsg = probe.expectMsgClass(TestMsg.class);
        assertEquals(doneMsg.content, receivedMsg.content, "The received message should match the sent message.");
        client.tell(doneMsg, probe.getRef());

        sys.terminate();
    }
}
