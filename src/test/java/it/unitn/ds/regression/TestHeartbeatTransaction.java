package it.unitn.ds.regression;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.javadsl.TestKit;
import it.unitn.ds.AbstractReplica.InitSystem;
import it.unitn.ds.HeartbeatTransaction.HeartbeatMsg;
import it.unitn.ds.HeartbeatTransaction.HeartbeatTickMsg;
import it.unitn.ds.Transaction.TransactionId;
import it.unitn.ds.Replica;

public class TestHeartbeatTransaction {
    
    @Test
    void coordinatorBroadcastsHeartbeatsPeriodically() {
        ActorSystem system = ActorSystem.create(
            "coordinatorBroadcastHeartbeatsPeriodically"
        );

        try {
            int coordinatorId = 0;
            int heartbeatIntervalMillis = 50;

            TestKit followerProbe = new TestKit(system);

            ActorRef coordinator = system.actorOf(
                Replica.props(
                    coordinatorId,
                    1,
                    5,
                    heartbeatIntervalMillis
                ),
                "coordinator"
            );

            int initialHeartbeatTransactionSequence = 0;
            TransactionId expectedHeartbeatTransactionId = new TransactionId(
                coordinator,
                initialHeartbeatTransactionSequence
            );


            Map<Integer, ActorRef> group = new HashMap<>();
            group.put(coordinatorId, coordinator);
            group.put(1, followerProbe.getRef());

            coordinator.tell(
                new InitSystem(group, coordinatorId),
                ActorRef.noSender()
            );

            HeartbeatMsg firstHeartbeat = followerProbe.expectMsgClass(
                Duration.ofSeconds(2),
                HeartbeatMsg.class
            );

            assertEquals(
                expectedHeartbeatTransactionId,
                firstHeartbeat.transactionId
            );

            assertEquals(
                coordinatorId,
                firstHeartbeat.coordinatorId
            );

            assertEquals(
                coordinator,
                firstHeartbeat.sender
            );

            HeartbeatMsg secondHeartbeat = followerProbe.expectMsgClass(
                Duration.ofSeconds(2),
                HeartbeatMsg.class
            );

            assertEquals(
                expectedHeartbeatTransactionId,
                secondHeartbeat.transactionId
            );

            assertEquals(
                coordinatorId,
                secondHeartbeat.coordinatorId
            );

            assertEquals(
                coordinator,
                secondHeartbeat.sender
            );

        } finally {
            TestKit.shutdownActorSystem(system);
        }
    }

    /**
     * Verifies that Replica dispatches heartbeat ticks using their transaction ID.
     * 
     * A tick with an unknown transaction ID must not produce a heartbeat.
     */
    @Test
    void heartbeatTickIsRoutedByTransactionId() {
        ActorSystem system = ActorSystem.create(
            "heartbeatTickIsRoutedByTransactionId"
        );

        try {
            int coordinatorId = 0;
            int heartbeatIntervalMillis = 5000;

            TestKit followerProbe = new TestKit(system);

            ActorRef coordinator = system.actorOf(
                Replica.props(
                    coordinatorId,
                    1,
                    5,
                    heartbeatIntervalMillis
                ),
                "coordinator"
            );

            Map<Integer, ActorRef> group = new HashMap<>();
            group.put(coordinatorId, coordinator);
            group.put(1, followerProbe.getRef());

            TransactionId activeTransactionId = new TransactionId(
                coordinator,
                0
            );

            TransactionId unknownTransactionId = new TransactionId(
                coordinator,
                999
            );

            coordinator.tell(
                new InitSystem(group, coordinatorId),
                ActorRef.noSender()
            );

            coordinator.tell(
                new HeartbeatTickMsg(
                    unknownTransactionId,
                    null,
                    coordinator
                ),
                ActorRef.noSender()
            );

            followerProbe.expectNoMessage(
                Duration.ofMillis(200)
            );

            coordinator.tell(
                new HeartbeatTickMsg(
                    activeTransactionId,
                    null,
                    coordinator
                ),
                ActorRef.noSender()
            );

            HeartbeatMsg heartbeat = followerProbe.expectMsgClass(
                Duration.ofSeconds(2),
                HeartbeatMsg.class
            );

            assertEquals(
                activeTransactionId,
                heartbeat.transactionId
            );
        } finally {
            TestKit.shutdownActorSystem(system);
        }
    }
}
