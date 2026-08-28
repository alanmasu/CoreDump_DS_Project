package it.unitn.ds.regression;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import akka.actor.Actor;
import akka.actor.ActorRef;
import it.unitn.ds.AbstractReplica;
import it.unitn.ds.AbstractReplica.CoordinatorElected;
import it.unitn.ds.ElectionTransaction;
import it.unitn.ds.ElectionTransaction.ElectionCandidate;
import it.unitn.ds.ElectionTransaction.SynchronizationMsg;
import it.unitn.ds.EpochPair;
import it.unitn.ds.TestsCommons;
import it.unitn.ds.TestsCommons.TestsSystemWrapper;
import it.unitn.ds.Transaction.TransactionId;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class TestElectionTransaction {

    private CoordinatorElected sendValidSynchronization(
            TestsSystemWrapper system, ActorRef follower, TransactionId synchronizationId, EpochPair epochPair) {
        SynchronizationMsg synchronization = new SynchronizationMsg(
                synchronizationId,
                epochPair,
                system.actors.get(2),
                0,
                2,
                epochPair,
                new int[AbstractReplica.POSITIONS_LIST_LENGTH]);
        follower.tell(synchronization, Actor.noSender());
        return system.probes.get(1).expectMsgClass(Duration.ofSeconds(1), CoordinatorElected.class);
    }

    @Test
    void newerSequenceWinsWhenEpochIsEqual() {
        ElectionCandidate older = new ElectionCandidate(3, new EpochPair(2, 3));

        ElectionCandidate newer = new ElectionCandidate(2, new EpochPair(2, 4));

        assertTrue(newer.compareTo(older) > 0, "Newer sequence should win");
        assertTrue(older.compareTo(newer) < 0, "Older sequence should lose");
    }

    @Test
    void newerEpochWinsOverHigherSequence() {
        ElectionCandidate olderEpoch = new ElectionCandidate(9, new EpochPair(2, 99));

        ElectionCandidate newerEpoch = new ElectionCandidate(0, new EpochPair(3, 0));

        assertTrue(newerEpoch.compareTo(olderEpoch) > 0, "Newer epoch should win");
        assertTrue(olderEpoch.compareTo(newerEpoch) < 0, "Older epoch should lose");
    }

    @Test
    void higherReplicaIdBreaksEqualEpochPairTie() {
        ElectionCandidate lowerId = new ElectionCandidate(2, new EpochPair(2, 4));

        ElectionCandidate higherId = new ElectionCandidate(5, new EpochPair(2, 4));

        assertTrue(higherId.compareTo(lowerId) > 0, "Higher replica ID should win ties");
        assertTrue(lowerId.compareTo(higherId) < 0, "Lower replica ID should lose ties");
    }

    @Test
    void equalCandidatesCompareAsEqual() {
        ElectionCandidate first = new ElectionCandidate(5, new EpochPair(2, 4));

        ElectionCandidate same = new ElectionCandidate(5, new EpochPair(2, 4));

        assertEquals(0, first.compareTo(same), "Equal candidates should compare equally");
        assertEquals(first, same, "Equal candidates should be equal by value");
        assertEquals(first.hashCode(), same.hashCode(), "Equal candidates need equal hash codes");
    }

    @Test
    void realUpdateBeatsCandidateWithNoObservedUpdate() {
        ElectionCandidate noUpdate = new ElectionCandidate(5);

        ElectionCandidate realUpdate = new ElectionCandidate(0, new EpochPair(0, 0));

        assertTrue(realUpdate.compareTo(noUpdate) > 0, "A candidate with an update should win");
        assertTrue(noUpdate.compareTo(realUpdate) < 0, "A candidate without an update should lose");
    }

    @Test
    void noUpdateCandidatesUseReplicaIdAsTieBreaker() {
        ElectionCandidate lowerId = new ElectionCandidate(2);

        ElectionCandidate higherId = new ElectionCandidate(5);

        assertTrue(higherId.compareTo(lowerId) > 0, "Higher replica ID should win ties");
        assertTrue(lowerId.compareTo(higherId) < 0, "Lower replica ID should lose ties");
    }

    @Test
    void ringNavigationHandlesWraparoundAndSkippedReplicas() {
        ElectionTransaction.RingNavigation ring = new ElectionTransaction.RingNavigation(List.of(0, 2, 3, 5));

        assertEquals(3, ring.nextReplicaId(2, Set.of()), "Ring should move to the next ID");
        assertEquals(0, ring.nextReplicaId(5, Set.of()), "Ring should wrap around");
        assertEquals(5, ring.nextReplicaId(2, Set.of(3)), "Ring should skip unavailable IDs");
        assertEquals(3, ring.nextReplicaId(5, Set.of(0, 2)), "Ring should skip and wrap around");
    }

    @Test
    void ringNavigationFailsWhenNoReplicaIsAvailable() {
        ElectionTransaction.RingNavigation ring = new ElectionTransaction.RingNavigation(List.of(0, 2, 3, 5));

        assertThrows(
                IllegalStateException.class,
                () -> ring.nextReplicaId(2, Set.of(0, 3, 5)),
                "Ring should fail when no replica is available");
    }

    @Test
    void electionMessageCopiesAndProtectsCandidateList() {
        ElectionCandidate candidate = new ElectionCandidate(3, new EpochPair(2, 4));

        List<ElectionCandidate> originalCandidates = new ArrayList<>(List.of(candidate));

        ElectionTransaction.ElectionMsg message =
                new ElectionTransaction.ElectionMsg(null, new EpochPair(2, 4), null, 1, originalCandidates);

        originalCandidates.clear();

        assertEquals(1, message.candidates.size(), "Message should copy the candidate list");
        assertSame(candidate, message.candidates.get(0), "Message should retain the candidate");

        assertThrows(
                UnsupportedOperationException.class,
                () -> message.candidates.add(candidate),
                "Message candidate list should be immutable");
    }

    @Test
    void electionAckPreservesTransactionMetadata() {
        TransactionId transactionId = new TransactionId(null, 7);
        EpochPair epochPair = new EpochPair(2, 4);

        ElectionTransaction.ElectionAckMsg ack = new ElectionTransaction.ElectionAckMsg(transactionId, epochPair, null);

        assertSame(transactionId, ack.transactionId, "ACK should preserve transaction ID");
        assertSame(epochPair, ack.epochPair, "ACK should preserve epoch pair");
    }

    @Test
    void electionAckTimeoutPreservesTargetAndAttemptVersion() {
        TransactionId transactionId = new TransactionId(null, 8);
        EpochPair epochPair = new EpochPair(2, 4);

        ElectionTransaction.ElectionAckTimeoutMsg timeout =
                new ElectionTransaction.ElectionAckTimeoutMsg(transactionId, epochPair, null, 5, 2L);

        assertSame(transactionId, timeout.transactionId, "Timeout should preserve transaction ID");
        assertSame(epochPair, timeout.epochPair, "Timeout should preserve epoch pair");
        assertEquals(5, timeout.expectedTargetId, "Timeout should preserve target ID");
        assertEquals(2L, timeout.attemptVersion, "Timeout should preserve attempt version");
    }

    @Test
    void synchronizationMessageProtectsPositionsSnapshot() {
        int[] originalPositions = {1, 2, 3};

        ElectionTransaction.SynchronizationMsg message = new ElectionTransaction.SynchronizationMsg(
                null, new EpochPair(3, 0), null, 1, 5, new EpochPair(3, 0), originalPositions);

        originalPositions[0] = 99;
        int[] firstRead = message.getPositions();
        firstRead[1] = 88;

        assertArrayEquals(
                new int[] {1, 2, 3}, message.getPositions(), "Synchronization should protect its positions snapshot");
    }

    @Test
    void synchronizationFromUnexpectedReplicaIsIgnored() {
        TestsSystemWrapper system = TestsCommons.createTestSystem("unexpectedSynchronizationSender", 3, 0, 1, 5);

        try {
            ActorRef follower = system.actors.get(1);
            SynchronizationMsg synchronization = new SynchronizationMsg(
                    new TransactionId(system.actors.get(1), -1),
                    new EpochPair(1, 0),
                    system.actors.get(1),
                    0,
                    2,
                    new EpochPair(1, 0),
                    new int[AbstractReplica.POSITIONS_LIST_LENGTH]);

            follower.tell(synchronization, Actor.noSender());

            assertDoesNotThrow(
                    () -> system.probes.get(1).expectNoMessage(Duration.ofMillis(250)),
                    "Unexpected synchronization should not notify the follower");
        } finally {
            system.system.terminate();
        }
    }

    @Test
    void invalidSynchronizationDoesNotPoisonElectionTerm() {
        TestsSystemWrapper system = TestsCommons.createTestSystem("invalidSynchronizationTerm", 3, 0, 1, 5);

        try {
            ActorRef follower = system.actors.get(1);
            TransactionId synchronizationId = new TransactionId(system.actors.get(2), -1);
            EpochPair newEpochPair = new EpochPair(1, 0);

            SynchronizationMsg invalidSynchronization = new SynchronizationMsg(
                    synchronizationId, newEpochPair, system.actors.get(2), 0, 2, newEpochPair, new int[0]);
            follower.tell(invalidSynchronization, Actor.noSender());
            assertDoesNotThrow(
                    () -> system.probes.get(1).expectNoMessage(Duration.ofMillis(250)),
                    "Invalid synchronization should be ignored");

            CoordinatorElected elected = sendValidSynchronization(system, follower, synchronizationId, newEpochPair);
            assertEquals(2, elected.newCoordinatorId, "Valid synchronization should elect replica 2");
            assertEquals(1, elected.replicaId, "Follower 1 should report the election");
        } finally {
            system.system.terminate();
        }
    }

    @Test
    void malformedElectionMessageIsIgnoredBeforeRouting() {
        TestsSystemWrapper system = TestsCommons.createTestSystem("malformedElectionMessage", 3, 0, 1, 5);

        try {
            ActorRef follower = system.actors.get(1);
            ElectionTransaction.ElectionMsg malformedElection = new ElectionTransaction.ElectionMsg(
                    null,
                    new EpochPair(0, 0),
                    system.actors.get(2),
                    0,
                    List.of(new ElectionCandidate(2, new EpochPair(0, 0))));
            follower.tell(malformedElection, Actor.noSender());
            assertDoesNotThrow(
                    () -> system.probes.get(1).expectNoMessage(Duration.ofMillis(250)),
                    "Malformed election should be ignored");

            EpochPair newEpochPair = new EpochPair(1, 0);
            CoordinatorElected elected = sendValidSynchronization(
                    system, follower, new TransactionId(system.actors.get(2), -1), newEpochPair);
            assertEquals(2, elected.newCoordinatorId, "Valid synchronization should elect replica 2");
        } finally {
            system.system.terminate();
        }
    }
}
