package it.unitn.ds.regression;

import static org.junit.jupiter.api.Assertions.assertEquals;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.javadsl.TestKit;
import it.unitn.ds.EpochPair;
import it.unitn.ds.Logger;
import it.unitn.ds.ProbeTransaction.ProbeMsg;
import it.unitn.ds.Replica;
import it.unitn.ds.Transaction.TransactionId;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

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
        ProbeMsg receivedMsg;

        EpochPair epochPair = new EpochPair(0, 0);
        TransactionId tId1 = new TransactionId(replica, 1);
        TransactionId tId2 = new TransactionId(replica, 2);

        ProbeMsg startMsg1 = new ProbeMsg(tId1, epochPair, probe.getRef(), "start");
        ProbeMsg startMsg2 = new ProbeMsg(tId2, epochPair, probe.getRef(), "start");
        ProbeMsg ackMsg1 = new ProbeMsg(tId1, epochPair, probe.getRef(), "ack");
        ProbeMsg ackMsg2 = new ProbeMsg(tId2, epochPair, probe.getRef(), "ack");
        replica.tell(startMsg1, probe.getRef());
        replica.tell(startMsg2, probe.getRef());

        // Expecting the replica to send back an ack for each start message
        // 1st ack
        receivedMsg = probe.expectMsgClass(ProbeMsg.class);
        assertEquals(receivedMsg.content, "ack", "The received message should be an ack");
        if (receivedMsg.transactionId.equals(tId1)) {
            replica.tell(ackMsg1, probe.getRef());
        } else {
            replica.tell(ackMsg2, probe.getRef());
        }

        // 2nd ack
        receivedMsg = probe.expectMsgClass(ProbeMsg.class);
        assertEquals(receivedMsg.content, "ack", "The received message should be an ack");
        if (receivedMsg.transactionId.equals(tId1)) {
            replica.tell(ackMsg1, probe.getRef());
        } else {
            replica.tell(ackMsg2, probe.getRef());
        }

        // Expecting the replica to send back a done message for each transaction
        ProbeMsg doneMsg1 = new ProbeMsg(tId1, epochPair, probe.getRef(), "done");
        ProbeMsg doneMsg2 = new ProbeMsg(tId2, epochPair, probe.getRef(), "done");

        // 1st done
        receivedMsg = probe.expectMsgClass(ProbeMsg.class);
        assertEquals(receivedMsg.content, "done", "The received message should match the sent message.");
        if (receivedMsg.transactionId.equals(tId1)) {
            replica.tell(doneMsg1, probe.getRef());
        } else {
            replica.tell(doneMsg2, probe.getRef());
        }

        // 2nd done
        receivedMsg = probe.expectMsgClass(ProbeMsg.class);
        assertEquals(receivedMsg.content, "done", "The received message should match the sent message.");
        if (receivedMsg.transactionId.equals(tId1)) {
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
        ProbeMsg receivedMsg;

        EpochPair epochPair = new EpochPair(0, 0);
        TransactionId tId1 = new TransactionId(client, 1);
        TransactionId tId2 = new TransactionId(client, 2);

        ProbeMsg startMsg1 = new ProbeMsg(tId1, epochPair, probe.getRef(), "start");
        ProbeMsg startMsg2 = new ProbeMsg(tId2, epochPair, probe.getRef(), "start");
        ProbeMsg ackMsg1 = new ProbeMsg(tId1, epochPair, probe.getRef(), "ack");
        ProbeMsg ackMsg2 = new ProbeMsg(tId2, epochPair, probe.getRef(), "ack");
        client.tell(startMsg1, probe.getRef());
        client.tell(startMsg2, probe.getRef());

        receivedMsg = probe.expectMsgClass(ProbeMsg.class);
        assertEquals(receivedMsg.content, ackMsg1.content, "The received message should be an ack");
        client.tell(ackMsg1, probe.getRef());

        ProbeMsg doneMsg1 = new ProbeMsg(tId1, epochPair, probe.getRef(), "done");
        receivedMsg = probe.expectMsgClass(ProbeMsg.class);
        assertEquals(doneMsg1.content, receivedMsg.content, "The received message should match the sent message.");
        client.tell(doneMsg1, probe.getRef());

        receivedMsg = probe.expectMsgClass(ProbeMsg.class);
        assertEquals(receivedMsg.content, ackMsg2.content, "The received message should be an ack");
        client.tell(ackMsg2, probe.getRef());

        ProbeMsg doneMsg2 = new ProbeMsg(tId2, epochPair, probe.getRef(), "done");
        receivedMsg = probe.expectMsgClass(ProbeMsg.class);
        assertEquals(doneMsg2.content, receivedMsg.content, "The received message should match the sent message.");
        client.tell(doneMsg2, probe.getRef());

        sys.terminate();
    }
}
