package it.unitn.ds.regression;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.javadsl.TestKit;
import it.unitn.ds.EpochPair;
import it.unitn.ds.Logger;
import it.unitn.ds.Replica;
import it.unitn.ds.TestMsg;
import it.unitn.ds.Transaction.TransactionId;

public class TestDispatcher {
    
    @Test
    public void testReplicaDispatcher() {
        ActorSystem sys = ActorSystem.create("TestDispatcher");
		TestKit probe = new TestKit(sys);
        
        Logger.setDestinationStdout();
        Logger.setDebugEnabled(true);
        Logger.setLoggingEnabled(true);

        ActorRef replica = sys.actorOf(Replica.propsWithListener(0, 1000, 1000, 1000, probe.getRef()), "replica");
        TestMsg startMsg = new TestMsg(new TransactionId(replica, 1), new EpochPair(0, 0), probe.getRef(), "start");     
        TestMsg ackMsg = new TestMsg(new TransactionId(replica, 1), new EpochPair(0, 0), probe.getRef(), "ack");     
        replica.tell(startMsg, probe.getRef());
        
        TestMsg receivedMsg = probe.expectMsgClass(TestMsg.class);
        assertEquals(ackMsg.content, receivedMsg.content, "The received message should be an ack");
        replica.tell(ackMsg, probe.getRef());
        
    
        TestMsg doneMsg = new TestMsg(new TransactionId(replica, 1), new EpochPair(0, 0), probe.getRef(), "done");
        receivedMsg = probe.expectMsgClass(TestMsg.class);
        assertEquals(doneMsg.content, receivedMsg.content, "The received message should match the sent message.");
        replica.tell(doneMsg, probe.getRef());

        sys.terminate();
    }

    @Test
    void testClientDispatcher() {
        ActorSystem sys = ActorSystem.create("TestDispatcher");
        TestKit probe = new TestKit(sys);

        Logger.setDestinationStdout();
        Logger.setDebugEnabled(true);
        Logger.setLoggingEnabled(true);

        ActorRef client = sys.actorOf(it.unitn.ds.Client.propsWithListener(1000, 1000, null, probe.getRef()), "client");
        TestMsg testMsg = new TestMsg(new TransactionId(client, 1), new EpochPair(0, 0), probe.getRef(), "Hello, Client!");
        client.tell(testMsg, probe.getRef());
        
        TestMsg receivedMsg = probe.expectMsgClass(TestMsg.class);
        assertEquals(testMsg, receivedMsg, "The received message should match the sent message.");

        TestMsg testMsg2 = new TestMsg(new TransactionId(client, 2), new EpochPair(0, 0), probe.getRef(), "Hello, Client!");
        receivedMsg = probe.expectMsgClass(TestMsg.class);
        assertEquals(testMsg2, receivedMsg, "The received message should match the sent message.");

        sys.terminate();
    }
}
