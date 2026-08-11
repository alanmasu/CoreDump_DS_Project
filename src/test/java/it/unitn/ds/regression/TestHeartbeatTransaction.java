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
import it.unitn.ds.HeartbeatTransaction.Heartbeat;
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

            Map<Integer, ActorRef> group = new HashMap<>();
            group.put(coordinatorId, coordinator);
            group.put(1, followerProbe.getRef());

            coordinator.tell(
                new InitSystem(group, coordinatorId),
                ActorRef.noSender()
            );

            Heartbeat firstHeartbeat = followerProbe.expectMsgClass(
                Duration.ofSeconds(2),
                Heartbeat.class
            );

            assertEquals(
                coordinatorId,
                firstHeartbeat.coordinatorId
            );

            Heartbeat secondHeartbeat = followerProbe.expectMsgClass(
                Duration.ofSeconds(2),
                Heartbeat.class
            );

            assertEquals(
                coordinatorId,
                secondHeartbeat.coordinatorId
            );

        } finally {
            TestKit.shutdownActorSystem(system);
        }
    }
}
