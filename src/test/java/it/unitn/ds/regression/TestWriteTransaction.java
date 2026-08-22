package it.unitn.ds.regression;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import java.util.Optional;

import akka.actor.Actor;
import akka.actor.ActorRef;
import it.unitn.ds.AbstractClient;
import it.unitn.ds.AbstractClient.WriteResult;
import it.unitn.ds.AbstractClient.WriteTimeout;
import it.unitn.ds.AbstractReplica;
import it.unitn.ds.AbstractReplica.Crash;
import it.unitn.ds.WriteTransaction.WriteFinishMsg;
import it.unitn.ds.Client;
import it.unitn.ds.Logger;
import it.unitn.ds.TestsCommons;
import it.unitn.ds.TestsCommons.TestsSystemWrapper;
import it.unitn.ds.Transaction.TransactionId;
import akka.testkit.javadsl.TestKit;


/**
 * This class test the WriteTransaction class.
 */
class TestWriteTransaction {
    
    final static int NODES_N = 3;
    final static int COORDINATOR_ID = 0;
    TestsSystemWrapper sys;
    TestKit probe;
    ActorRef replica;
    ActorRef client;

    @AfterEach
    void teardown() {
        sys.system.terminate();
    }

    @BeforeEach
    void setup() {
        sys = TestsCommons.createTestSystem("oneClientWrite_" + COORDINATOR_ID, NODES_N, COORDINATOR_ID);
        probe = new TestKit(sys.system);
        replica = sys.actors.get(0);
        client = sys.system.actorOf(   Client.propsWithListener(sys.client_read_timeout, 
                                                                         sys.client_write_timeout,
                                                                         Optional.ofNullable(sys.actors.get(0)),
                                                                         probe.getRef()),
                                                "client1");
        Logger.setDestinationStdout();
        Logger.setDebugEnabled(true);
        Logger.setLoggingEnabled(true);
    }

    @Test
    void testSimpleWriteTransaction(){
		client.tell(new AbstractClient.WriteRequest(TestsCommons.TEST_INDEX, TestsCommons.TEST_VALUE, replica), Actor.noSender());

        TransactionId clientTransactionId = new TransactionId(client, 0);
        // WriteFinishMsg finishMsg =;
        sys.system.scheduler().scheduleOnce( Duration.ofMillis(TestsCommons.getMaxUpdateDelay(sys)*3),
                                             replica,
                                             new WriteFinishMsg(clientTransactionId, null, replica), 
                                             sys.system.dispatcher(),
                                             ActorRef.noSender());

        WriteResult result = probe.expectMsgClass(WriteResult.class);
        assertEquals(TestsCommons.TEST_VALUE, result.value, "The value written should be the same as the one requested");
        assertEquals(TestsCommons.TEST_INDEX, result.index, "The index written should be the same as the one requested");
        assertEquals(true, result.success, "The write should be successful");
	}
    

    @Test
    void testWriteTransactionTimeout(){
        Crash replicaCrash = new Crash(Crash.Type.Now, 0);
        replica.tell(replicaCrash, ActorRef.noSender());
        client.tell(new AbstractClient.WriteRequest(TestsCommons.TEST_INDEX, TestsCommons.TEST_VALUE, replica), Actor.noSender());
            
        probe.expectNoMessage(Duration.ofMillis(TestsCommons.getClientWriteTimeout(AbstractReplica.MAX_LATENCY, sys.getNNodes())));
        WriteTimeout result = probe.expectMsgClass(WriteTimeout.class);
        assertEquals(TestsCommons.TEST_INDEX, result.index, "The index written should be the same as the one requested");
        assertEquals(client, result.client, "The client should be the same as the one requested");
        assertEquals(replica, result.replica, "The replica should be the same as the one requested");
        sys.system.terminate();
    }

}
