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
        ActorRef replica = sys.actorOf(Replica.propsWithListener(0, 100, 1000, 1000, probe.getRef()), "replica");
        TestMsg receivedMsg;

        EpochPair epochPair = new EpochPair(0, 0);
        TransactionId tId1 = new TransactionId(replica, 1);
        TransactionId tId2 = new TransactionId(replica, 2);

        TestMsg startMsg1 = new TestMsg(tId1, epochPair, probe.getRef(), "start");
        TestMsg startMsg2 = new TestMsg(tId2, epochPair, probe.getRef(), "start");
        TestMsg ackMsg1 = new TestMsg(tId1, epochPair, probe.getRef(), "ack");
        TestMsg ackMsg2 = new TestMsg(tId2, epochPair, probe.getRef(), "ack");
        replica.tell(startMsg1, probe.getRef());
        replica.tell(startMsg2, probe.getRef());
        

        // Expecting the replica to send back an ack for each start message
        // 1st ack
        receivedMsg = probe.expectMsgClass(TestMsg.class);
        assertEquals(receivedMsg.content, "ack", "The received message should be an ack");
        if(receivedMsg.transactionId.equals(tId1)) {
            replica.tell(ackMsg1, probe.getRef());
        } else {
            replica.tell(ackMsg2, probe.getRef());
        }

        // 2nd ack
        receivedMsg = probe.expectMsgClass(TestMsg.class);
        assertEquals(receivedMsg.content, "ack", "The received message should be an ack");
        if(receivedMsg.transactionId.equals(tId1)) {
            replica.tell(ackMsg1, probe.getRef());
        } else {
            replica.tell(ackMsg2, probe.getRef());
        }
        
    
        // Expecting the replica to send back a done message for each transaction
        TestMsg doneMsg1 = new TestMsg(tId1, epochPair, probe.getRef(), "done");
        TestMsg doneMsg2 = new TestMsg(tId2, epochPair, probe.getRef(), "done");

        // 1st done
        receivedMsg = probe.expectMsgClass(TestMsg.class);
        assertEquals(receivedMsg.content, "done", "The received message should match the sent message.");
        if(receivedMsg.transactionId.equals(tId1)) {
            replica.tell(doneMsg1, probe.getRef());
        } else {
            replica.tell(doneMsg2, probe.getRef());
        }

        // 2nd done
        receivedMsg = probe.expectMsgClass(TestMsg.class);
        assertEquals(receivedMsg.content, "done", "The received message should match the sent message.");
        if(receivedMsg.transactionId.equals(tId1)) {
            replica.tell(doneMsg1, probe.getRef());
        } else {
            replica.tell(doneMsg2, probe.getRef());
        }

        sys.terminate();
    }

    @Test
    void testClientDispatcher() {
        ActorSystem sys = ActorSystem.create("TestDispatcher");
        TestKit probe = new TestKit(sys);
        ActorRef client = sys.actorOf(it.unitn.ds.Client.propsWithListener(1000, 1000, null, probe.getRef()), "client");
        TestMsg receivedMsg;

        EpochPair epochPair = new EpochPair(0, 0);
        TransactionId tId1 = new TransactionId(client, 1);
        TransactionId tId2 = new TransactionId(client, 2);

        TestMsg startMsg1 = new TestMsg(tId1, epochPair, probe.getRef(), "start");
        TestMsg startMsg2 = new TestMsg(tId2, epochPair, probe.getRef(), "start");
        TestMsg ackMsg1 = new TestMsg(tId1, epochPair, probe.getRef(), "ack");
        TestMsg ackMsg2 = new TestMsg(tId2, epochPair, probe.getRef(), "ack");
        client.tell(startMsg1, probe.getRef());
        client.tell(startMsg2, probe.getRef());

        receivedMsg = probe.expectMsgClass(TestMsg.class);
        assertEquals(receivedMsg, ackMsg1, "The received message should be an ack");
        client.tell(ackMsg1, probe.getRef());

        TestMsg doneMsg1 = new TestMsg(tId1 , epochPair, probe.getRef(), "done");
        receivedMsg = probe.expectMsgClass(TestMsg.class);
        assertEquals(doneMsg1, receivedMsg, "The received message should match the sent message.");
        client.tell(doneMsg1, probe.getRef());

        receivedMsg = probe.expectMsgClass(TestMsg.class);
        assertEquals(receivedMsg, ackMsg2, "The received message should be an ack");
        client.tell(ackMsg2, probe.getRef());

        TestMsg doneMsg2 = new TestMsg(tId2 , epochPair, probe.getRef(), "done");
        receivedMsg = probe.expectMsgClass(TestMsg.class);
        assertEquals(doneMsg2, receivedMsg, "The received message should match the sent message.");
        client.tell(doneMsg2, probe.getRef());

        sys.terminate();
    }
}
