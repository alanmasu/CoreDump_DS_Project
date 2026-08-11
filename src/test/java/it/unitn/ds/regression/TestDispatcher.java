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
        TestMsg testMsg = new TestMsg(new TransactionId(replica, 1), new EpochPair(0, 0), probe.getRef(), "Hello, Replica!");     
        replica.tell(testMsg, probe.getRef());
        
        TestMsg receivedMsg = probe.expectMsgClass(TestMsg.class);
        assertEquals(testMsg, receivedMsg, "The received message should match the sent message.");
        
        TestMsg testMsg2 = new TestMsg(new TransactionId(replica, 2), new EpochPair(0, 0), probe.getRef(), "Hello, Replica!");
        receivedMsg = probe.expectMsgClass(TestMsg.class);
        assertEquals(testMsg2, receivedMsg, "The received message should match the sent message.");

        sys.terminate();
    }
}
