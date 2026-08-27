package it.unitn.ds;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.TestActorRef;
import akka.testkit.javadsl.TestKit;
import it.unitn.ds.AbstractClient.ReadRequest;
import it.unitn.ds.AbstractClient.ReadResult;
import it.unitn.ds.AbstractClient.WriteRequest;
import it.unitn.ds.AbstractClient.WriteResult;
import it.unitn.ds.AbstractReplica.CoordinatorElected;
import it.unitn.ds.AbstractReplica.Crash;
import it.unitn.ds.AbstractReplica.UpdateApplied;
import it.unitn.ds.TestsCommons.TestsSystemWrapper;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

class TestSequentialConsistency {

    private static final int REPLICA_COUNT = 5;
    private static final int COORDINATOR_ID = 0;
    private static final int INDEX = 0;

    @Test
    void coordinatorAllocatesOneMonotonicIdentityPerUpdateAndResetsForANewEpoch() {
        ActorSystem system = ActorSystem.create("coordinatorUpdateIdentity");

        try {
            TestActorRef<Replica> coordinatorRef = TestActorRef.create(system, Replica.props(0, 1, 5, 1000));
            Replica coordinator = coordinatorRef.underlyingActor();

            assertEquals(new Transaction.TransactionId(coordinatorRef, 1), coordinator.getNextTransactionId());
            assertEquals(new EpochPair(0, 1), coordinator.reserveNextUpdateEpochPair());
            assertEquals(new EpochPair(0, 2), coordinator.reserveNextUpdateEpochPair());
            assertEquals(new EpochPair(0, 3), coordinator.reserveNextUpdateEpochPair());

            coordinator.setEpochPair(new EpochPair(1, 0));

            assertEquals(new EpochPair(1, 1), coordinator.reserveNextUpdateEpochPair());
        } finally {
            system.terminate();
        }
    }

    @Test
    void concurrentClientsProduceTheSameUpdateOrderAtEveryReplica() {
        TestsSystemWrapper system =
                TestsCommons.createTestSystem("sameTotalOrderAtEveryReplica", REPLICA_COUNT, COORDINATOR_ID, 1, 8, 500);

        try {
            ActorRef oddClient = createClient(system, "oddClient");
            ActorRef evenClient = createClient(system, "evenClient");

            for (int value = 1; value <= 6; value += 2) {
                oddClient.tell(new WriteRequest(INDEX, value, system.actors.get(1)), ActorRef.noSender());
            }
            for (int value = 2; value <= 6; value += 2) {
                evenClient.tell(new WriteRequest(INDEX, value, system.actors.get(3)), ActorRef.noSender());
            }

            List<Integer> referenceOrder = appliedValues(system.probes.get(0), 6);
            assertEquals(Set.of(1, 2, 3, 4, 5, 6), new HashSet<>(referenceOrder));
            assertProgramOrder(referenceOrder, List.of(1, 3, 5));
            assertProgramOrder(referenceOrder, List.of(2, 4, 6));

            for (int replicaId = 1; replicaId < REPLICA_COUNT; replicaId++) {
                assertEquals(
                        referenceOrder,
                        appliedValues(system.probes.get(replicaId), 6),
                        "Every replica must apply the coordinator's exact total order");
            }
        } finally {
            system.system.terminate();
        }
    }

    @Test
    void aReadIssuedAfterAWriteByTheSameClientObservesThatWrite() {
        TestsSystemWrapper system =
                TestsCommons.createTestSystem("writeThenReadProgramOrder", 3, COORDINATOR_ID, 1, 8, 500);
        TestKit clientProbe = new TestKit(system.system);

        try {
            ActorRef targetReplica = system.actors.get(1);
            ActorRef client = system.system.actorOf(
                    Client.propsWithListener(
                            system.client_read_timeout,
                            system.client_write_timeout,
                            Optional.of(targetReplica),
                            clientProbe.getRef()),
                    "writeThenReadClient");

            client.tell(new WriteRequest(INDEX, 91), ActorRef.noSender());
            client.tell(new ReadRequest(INDEX), ActorRef.noSender());

            WriteResult write = clientProbe.expectMsgClass(Duration.ofSeconds(5), WriteResult.class);
            ReadResult read = clientProbe.expectMsgClass(Duration.ofSeconds(5), ReadResult.class);

            assertTrue(write.success);
            assertTrue(read.success);
            assertEquals(91, read.value);
            assertEquals(1, read.fromReplica);
        } finally {
            system.system.terminate();
        }
    }

    @Test
    void aWriteOkInterruptedByACoordinatorCrashIsRecoveredAsOneCommonPrefix() {
        TestsSystemWrapper system =
                TestsCommons.createTestSystem("recoverInterruptedWriteOk", 5, COORDINATOR_ID, 1, 8, 100);

        try {
            ActorRef coordinator = system.actors.get(COORDINATOR_ID);
            coordinator.tell(new Crash(Crash.Type.WriteOK, 1), ActorRef.noSender());
            system.probes.get(COORDINATOR_ID).expectMsgClass(Crash.class);

            ActorRef writer = createClient(system, "recoveryWriter");
            writer.tell(new WriteRequest(INDEX, 42, system.actors.get(1)), ActorRef.noSender());

            Map<Integer, Integer> electedCoordinators = new HashMap<>();
            for (int replicaId = 1; replicaId < 5; replicaId++) {
                CoordinatorElected elected = (CoordinatorElected) system.probes
                        .get(replicaId)
                        .fishForMessage(
                                Duration.ofSeconds(10),
                                "wait for synchronization",
                                message -> message instanceof CoordinatorElected);
                electedCoordinators.put(replicaId, elected.newCoordinatorId);
            }

            TestKit readProbe = new TestKit(system.system);
            for (int replicaId = 1; replicaId < 5; replicaId++) {
                ActorRef reader = system.system.actorOf(
                        Client.propsWithListener(
                                system.client_read_timeout,
                                system.client_write_timeout,
                                Optional.of(system.actors.get(replicaId)),
                                readProbe.getRef()),
                        "recoveryReader" + replicaId);
                reader.tell(new ReadRequest(INDEX), ActorRef.noSender());
            }

            Map<Integer, Integer> recoveredValues = new HashMap<>();
            for (int i = 1; i < 5; i++) {
                ReadResult result = readProbe.expectMsgClass(Duration.ofSeconds(5), ReadResult.class);
                recoveredValues.put(result.fromReplica, result.value);
            }

            assertEquals(1, new HashSet<>(electedCoordinators.values()).size());
            assertEquals(Set.of(1, 2, 3, 4), recoveredValues.keySet());
            assertEquals(
                    Set.of(42),
                    new HashSet<>(recoveredValues.values()),
                    "elected=" + electedCoordinators + ", recovered=" + recoveredValues);
        } finally {
            system.system.terminate();
        }
    }

    private ActorRef createClient(TestsSystemWrapper system, String name) {
        return system.system.actorOf(
                Client.propsWithListener(
                        system.client_read_timeout, system.client_write_timeout, Optional.empty(), null),
                name);
    }

    private List<Integer> appliedValues(TestKit replicaProbe, int count) {
        List<Integer> values = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            UpdateApplied applied = replicaProbe.expectMsgClass(Duration.ofSeconds(5), UpdateApplied.class);
            values.add(applied.value);
        }
        return values;
    }

    private void assertProgramOrder(List<Integer> totalOrder, List<Integer> clientOrder) {
        int previousIndex = -1;
        for (int value : clientOrder) {
            int currentIndex = totalOrder.indexOf(value);
            assertTrue(currentIndex > previousIndex, "The total order must preserve each client's program order");
            previousIndex = currentIndex;
        }
    }
}
