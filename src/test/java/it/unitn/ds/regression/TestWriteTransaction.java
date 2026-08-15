package it.unitn.ds.regression;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import java.util.Optional;

import akka.actor.Actor;
import akka.actor.ActorRef;
import it.unitn.ds.AbstractClient;
import it.unitn.ds.AbstractClient.WriteResult;
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
public class TestWriteTransaction {
    
    @BeforeAll
    static void setup() {
        Logger.setDestinationStdout();
        Logger.setDebugEnabled(true);
        Logger.setLoggingEnabled(true);
    }

    @Test
    void testSimpleWriteTransaction(){
        final int n_nodes = 3;
        final int coordinator = 0;
        final TestsSystemWrapper sys = TestsCommons.createTestSystem("oneClientWrite_" + coordinator, n_nodes, coordinator);
        setup();
		TestKit probe = new TestKit(sys.system);
        ActorRef replica = sys.actors.get(0);
		ActorRef client = sys.system.actorOf(   Client.propsWithListener(sys.client_read_timeout, 
                                                                         sys.client_write_timeout,
                                                                         Optional.ofNullable(sys.actors.get(0)),
                                                                         probe.getRef()),
				                                "client1");

		client.tell(new AbstractClient.WriteRequest(TestsCommons.TEST_INDEX, TestsCommons.TEST_VALUE, replica), Actor.noSender());

        TransactionId clientTransactionId = new TransactionId(client, 0);
        // WriteFinishMsg finishMsg =;
        sys.system.scheduler().scheduleOnce( Duration.ofMillis(TestsCommons.getMaxUpdateDelay(sys)*3),
                                             replica,
                                             new WriteFinishMsg(clientTransactionId, null, replica), 
                                             sys.system.dispatcher(),
                                             ActorRef.noSender());

        WriteResult result = probe.expectMsgClass(WriteResult.class);
        assertEquals(result.value, TestsCommons.TEST_VALUE, "The value written should be the same as the one requested");
        assertEquals(result.index, TestsCommons.TEST_INDEX, "The index written should be the same as the one requested");
        assertEquals(result.success, true, "The write should be successful");
		
		sys.system.terminate();
	}
    
}
