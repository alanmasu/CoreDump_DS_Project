package it.unitn.ds.regression;

import static org.junit.jupiter.api.Assertions.assertEquals;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.javadsl.TestKit;
import com.typesafe.config.ConfigFactory;
import it.unitn.ds.EpochPair;
import it.unitn.ds.Logger;
import it.unitn.ds.ProbeTransaction;
import it.unitn.ds.ProbeTransaction.ProbeMsg;
import it.unitn.ds.Replica;
import it.unitn.ds.Transaction.TransactionId;
import java.time.Duration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class TestDispatcher {

    private static final String MSG_EXPECT_ACK = "The received message should be an ack";
    private static final String MSG_EXPECT_MATCH = "The received message should match the sent message.";

    @BeforeAll
    static void setup() {
        Logger.setDestinationStdout();
        Logger.setDebugEnabled(true);
        Logger.setLoggingEnabled(true);
    }

    @Test
    void testReplicaDispatcher() {
        ActorSystem sys = ActorSystem.create("TestDispatcher");
        TestKit probe = new TestKit(sys);
        ActorRef replica = sys.actorOf(Replica.propsWithListener(0, 100, 1000, 1000, probe.getRef()), "replica");
        ProbeMsg receivedMsg;

        EpochPair epochPair = new EpochPair(0, 0);
        TransactionId tId1 = new TransactionId(replica, 1);
        TransactionId tId2 = new TransactionId(replica, 2);

        ProbeMsg startMsg1 = new ProbeMsg(tId1, epochPair, probe.getRef(), ProbeTransaction.MSG_START);
        ProbeMsg startMsg2 = new ProbeMsg(tId2, epochPair, probe.getRef(), ProbeTransaction.MSG_START);
        ProbeMsg ackMsg1 = new ProbeMsg(tId1, epochPair, probe.getRef(), ProbeTransaction.MSG_ACK);
        ProbeMsg ackMsg2 = new ProbeMsg(tId2, epochPair, probe.getRef(), ProbeTransaction.MSG_ACK);
        replica.tell(startMsg1, probe.getRef());
        replica.tell(startMsg2, probe.getRef());

        // Expecting the replica to send back an ack for each start message
        // 1st ack
        receivedMsg = probe.expectMsgClass(ProbeMsg.class);
        assertEquals(ProbeTransaction.MSG_ACK, receivedMsg.content, MSG_EXPECT_ACK);
        if (receivedMsg.transactionId.equals(tId1)) {
            replica.tell(ackMsg1, probe.getRef());
        } else {
            replica.tell(ackMsg2, probe.getRef());
        }

        // 2nd ack
        receivedMsg = probe.expectMsgClass(ProbeMsg.class);
        assertEquals(ProbeTransaction.MSG_ACK, receivedMsg.content, MSG_EXPECT_ACK);
        if (receivedMsg.transactionId.equals(tId1)) {
            replica.tell(ackMsg1, probe.getRef());
        } else {
            replica.tell(ackMsg2, probe.getRef());
        }

        // Expecting the replica to send back a done message for each transaction
        ProbeMsg doneMsg1 = new ProbeMsg(tId1, epochPair, probe.getRef(), ProbeTransaction.MSG_DONE);
        ProbeMsg doneMsg2 = new ProbeMsg(tId2, epochPair, probe.getRef(), ProbeTransaction.MSG_DONE);

        // 1st done
        receivedMsg = probe.expectMsgClass(ProbeMsg.class);
        assertEquals(ProbeTransaction.MSG_DONE, receivedMsg.content, MSG_EXPECT_MATCH);
        if (receivedMsg.transactionId.equals(tId1)) {
            replica.tell(doneMsg1, probe.getRef());
        } else {
            replica.tell(doneMsg2, probe.getRef());
        }

        // 2nd done
        receivedMsg = probe.expectMsgClass(ProbeMsg.class);
        assertEquals(ProbeTransaction.MSG_DONE, receivedMsg.content, MSG_EXPECT_MATCH);
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

        ProbeMsg startMsg1 = new ProbeMsg(tId1, epochPair, probe.getRef(), ProbeTransaction.MSG_START);
        ProbeMsg startMsg2 = new ProbeMsg(tId2, epochPair, probe.getRef(), ProbeTransaction.MSG_START);
        ProbeMsg ackMsg1 = new ProbeMsg(tId1, epochPair, probe.getRef(), ProbeTransaction.MSG_ACK);
        ProbeMsg ackMsg2 = new ProbeMsg(tId2, epochPair, probe.getRef(), ProbeTransaction.MSG_ACK);
        client.tell(startMsg1, probe.getRef());
        client.tell(startMsg2, probe.getRef());

        receivedMsg = probe.expectMsgClass(ProbeMsg.class);
        assertEquals(ackMsg1.content, receivedMsg.content, MSG_EXPECT_ACK);
        client.tell(ackMsg1, probe.getRef());

        ProbeMsg doneMsg1 = new ProbeMsg(tId1, epochPair, probe.getRef(), ProbeTransaction.MSG_DONE);
        receivedMsg = probe.expectMsgClass(ProbeMsg.class);
        assertEquals(doneMsg1.content, receivedMsg.content, MSG_EXPECT_MATCH);
        client.tell(doneMsg1, probe.getRef());

        receivedMsg = probe.expectMsgClass(ProbeMsg.class);
        assertEquals(ackMsg2.content, receivedMsg.content, MSG_EXPECT_ACK);
        client.tell(ackMsg2, probe.getRef());

        ProbeMsg doneMsg2 = new ProbeMsg(tId2, epochPair, probe.getRef(), ProbeTransaction.MSG_DONE);
        receivedMsg = probe.expectMsgClass(ProbeMsg.class);
        assertEquals(doneMsg2.content, receivedMsg.content, MSG_EXPECT_MATCH);
        client.tell(doneMsg2, probe.getRef());

        sys.terminate();
    }

    /**
     * A message arriving while the client has no transaction in flight must be dropped.
     * <p>
     * {@code Client.onTransactionComplete} sets {@code currentTransaction} to null once the
     * schedule drains, so an unguarded dereference in {@code onMessage} throws on any late
     * or duplicate reply that lands while the client is idle.
     * <p>
     * The actor system is built with a stopping supervisor strategy on purpose: under the
     * default one Akka restarts a failed actor and swallows the exception, so the probe
     * would observe silence either way and the test would pass even against the bug. With
     * this strategy a failure stops the actor, and the watching probe receives
     * {@code Terminated} — which is what {@code expectNoMessage} below rules out.
     */
    @Test
    void testClientDropsMessageWhenIdle() {
        ActorSystem sys = ActorSystem.create(
                "TestDispatcherIdle",
                ConfigFactory.parseString(
                                "akka.actor.guardian-supervisor-strategy = \"akka.actor.StoppingSupervisorStrategy\"")
                        .withFallback(ConfigFactory.load()));
        TestKit probe = new TestKit(sys);
        ActorRef client =
                sys.actorOf(it.unitn.ds.Client.propsWithListener(1000, 1000, null, probe.getRef()), "clientIdle");
        probe.watch(client);

        EpochPair epochPair = new EpochPair(0, 0);
        TransactionId tId1 = new TransactionId(client, 1);

        // No transaction has been started: this reply belongs to nobody.
        client.tell(new ProbeMsg(tId1, epochPair, probe.getRef(), ProbeTransaction.MSG_ACK), probe.getRef());
        // Neither a reply nor a Terminated: the message was dropped, the actor survived.
        probe.expectNoMessage(Duration.ofMillis(500));

        // The client must still be able to run a transaction afterwards.
        client.tell(new ProbeMsg(tId1, epochPair, probe.getRef(), ProbeTransaction.MSG_START), probe.getRef());
        ProbeMsg receivedMsg = probe.expectMsgClass(ProbeMsg.class);
        assertEquals(
                ProbeTransaction.MSG_ACK,
                receivedMsg.content,
                "The client should start the transaction normally after the dropped message");

        sys.terminate();
    }

    /**
     * A message must reach a transaction only if it belongs to it.
     * <p>
     * Without the id check, a reply that outlives its own transaction is handed to whichever
     * transaction happens to be current: the client would answer ProbeTransaction.MSG_DONE for a transaction
     * that never ran, and the running one would advance on somebody else's data. With
     * emulated network latency this reordering is routine rather than exceptional.
     */
    @Test
    void testClientDropsMessageOfForeignTransaction() {
        ActorSystem sys = ActorSystem.create("TestDispatcher");
        TestKit probe = new TestKit(sys);
        ActorRef client =
                sys.actorOf(it.unitn.ds.Client.propsWithListener(1000, 1000, null, probe.getRef()), "clientForeign");

        EpochPair epochPair = new EpochPair(0, 0);
        TransactionId tId1 = new TransactionId(client, 1);
        TransactionId tId2 = new TransactionId(client, 2);

        // Transaction 1 is the only one in flight.
        client.tell(new ProbeMsg(tId1, epochPair, probe.getRef(), ProbeTransaction.MSG_START), probe.getRef());
        ProbeMsg receivedMsg = probe.expectMsgClass(ProbeMsg.class);
        assertEquals(ProbeTransaction.MSG_ACK, receivedMsg.content, "The client should start transaction 1");
        assertEquals(tId1, receivedMsg.transactionId, "The start should belong to transaction 1");

        // A reply carrying transaction 2's id must not be routed to transaction 1.
        client.tell(new ProbeMsg(tId2, epochPair, probe.getRef(), ProbeTransaction.MSG_ACK), probe.getRef());
        probe.expectNoMessage(Duration.ofMillis(500));

        // Transaction 1 must be untouched and still able to progress.
        client.tell(new ProbeMsg(tId1, epochPair, probe.getRef(), ProbeTransaction.MSG_ACK), probe.getRef());
        receivedMsg = probe.expectMsgClass(ProbeMsg.class);
        assertEquals(ProbeTransaction.MSG_DONE, receivedMsg.content, "Transaction 1 should still progress normally");
        assertEquals(tId1, receivedMsg.transactionId, "The reply should belong to transaction 1");

        sys.terminate();
    }
}
