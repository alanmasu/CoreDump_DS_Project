package it.unitn.ds.regression;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Optional;

import org.junit.jupiter.api.Test;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.javadsl.TestKit;
import it.unitn.ds.Client;
import it.unitn.ds.EpochPair;
import it.unitn.ds.Logger;
import it.unitn.ds.Replica.ReadMsg;
import it.unitn.ds.TestsCommons;
import it.unitn.ds.TestsCommons.TestsSystemWrapper;
import it.unitn.ds.AbstractClient.ReadResult;
import it.unitn.ds.Transaction.TransactionId;

public class TestDispatcher {
    
    @Test
    public void testDispatcher() {
        int coordinator = 0;
        int n_nodes = 1;
        final TestsSystemWrapper sysWrapper = TestsCommons.createTestSystem("oneClientWriteWaitRead_" + coordinator, n_nodes, coordinator);
        ActorSystem sys = sysWrapper.system;
		TestKit probe = new TestKit(sysWrapper.system);
        
        Logger.setDestinationStdout();
        Logger.setDebugEnabled(true);
        Logger.setLoggingEnabled(true);

        ActorRef client = sys.actorOf(Client.propsWithListener(1000, 1000, Optional.empty(), probe.getRef()), "client");
        ActorRef replica = sysWrapper.actors.get(0);

        ReadMsg msg = new ReadMsg(new TransactionId(client, 0), new EpochPair(0, 0), client, 5);

        replica.tell(msg, client);
        
        ReadResult response = probe.expectMsgClass(ReadResult.class);

        assertEquals(new ReadResult(true, msg.index, 0, 0), response);

    }
}
