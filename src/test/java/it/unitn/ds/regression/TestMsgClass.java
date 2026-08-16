package it.unitn.ds.regression;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.javadsl.TestKit;
import it.unitn.ds.EpochPair;
import it.unitn.ds.Msg;
import it.unitn.ds.Transaction.TransactionId;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Regression tests for the methods overridden by {@link Msg}: equals, hashCode and toString.
 */
public class TestMsgClass {

    /** Minimal concrete message, used to exercise the abstract base class. */
    private static class SimpleMsg extends Msg {
        SimpleMsg(TransactionId transactionId, EpochPair epochPair, ActorRef sender) {
            super(transactionId, epochPair, sender);
        }
    }

    /** A sibling message type carrying exactly the same fields. */
    private static class OtherMsg extends Msg {
        OtherMsg(TransactionId transactionId, EpochPair epochPair, ActorRef sender) {
            super(transactionId, epochPair, sender);
        }
    }

    private static ActorSystem system;
    private static ActorRef alice;
    private static ActorRef bob;

    @BeforeAll
    static void setUp() {
        system = ActorSystem.create("TestMsgClass");
        alice = new TestKit(system).getRef();
        bob = new TestKit(system).getRef();
    }

    @AfterAll
    static void tearDown() {
        TestKit.shutdownActorSystem(system);
        system = null;
    }

    @Test
    void testEqualsAndHashCode() {
        SimpleMsg msg = new SimpleMsg(new TransactionId(alice, 1), new EpochPair(0, 0), alice);
        SimpleMsg sameValue = new SimpleMsg(new TransactionId(alice, 1), new EpochPair(0, 0), alice);

        assertEquals(msg, sameValue);
        assertEquals(msg.hashCode(), sameValue.hashCode());

        // A difference on any field, or a different message class, breaks equality
        assertNotEquals(msg, new SimpleMsg(new TransactionId(alice, 2), new EpochPair(0, 0), alice));
        assertNotEquals(msg, new SimpleMsg(new TransactionId(bob, 1), new EpochPair(0, 0), alice));
        assertNotEquals(msg, new SimpleMsg(new TransactionId(alice, 1), new EpochPair(1, 0), alice));
        assertNotEquals(msg, new SimpleMsg(new TransactionId(alice, 1), new EpochPair(0, 0), bob));
        assertNotEquals(msg, new OtherMsg(new TransactionId(alice, 1), new EpochPair(0, 0), alice));
        assertNotEquals(msg, null);
    }

    @Test
    void testEqualsWithNullEpochPairAndSender() {
        SimpleMsg bothNull = new SimpleMsg(new TransactionId(alice, 1), null, null);
        SimpleMsg bothNullTwin = new SimpleMsg(new TransactionId(alice, 1), null, null);
        SimpleMsg complete = new SimpleMsg(new TransactionId(alice, 1), new EpochPair(0, 0), alice);

        assertEquals(bothNull, bothNullTwin);
        assertEquals(bothNull.hashCode(), bothNullTwin.hashCode());
        assertNotEquals(bothNull, complete);
        assertNotEquals(complete, bothNull);
    }

    @Test
    void testToString() {
        SimpleMsg msg = new SimpleMsg(new TransactionId(alice, 7), new EpochPair(1, 2), alice);
        String rendered = msg.toString();

        assertTrue(rendered.contains("tId=<" + alice.path().name() + ", 7>"), rendered);
        assertTrue(rendered.contains("epochPair=<1, 2>"), rendered);
        assertTrue(rendered.contains("sender=" + alice.path()), rendered);

        SimpleMsg withoutSender = new SimpleMsg(new TransactionId(alice, 7), new EpochPair(1, 2), null);
        assertTrue(withoutSender.toString().contains("sender=none"), withoutSender.toString());
    }
}
