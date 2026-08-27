package it.unitn.ds.regression;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
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

    @Test
    void newerSequenceWinsWhenEpochIsEqual() {
        ElectionCandidate older = new ElectionCandidate(3, new EpochPair(2, 3));

        ElectionCandidate newer = new ElectionCandidate(2, new EpochPair(2, 4));

        assertTrue(newer.compareTo(older) > 0);
        assertTrue(older.compareTo(newer) < 0);
    }

    @Test
    void newerEpochWinsOverHigherSequence() {
        ElectionCandidate olderEpoch = new ElectionCandidate(9, new EpochPair(2, 99));

        ElectionCandidate newerEpoch = new ElectionCandidate(0, new EpochPair(3, 0));

        assertTrue(newerEpoch.compareTo(olderEpoch) > 0);
        assertTrue(olderEpoch.compareTo(newerEpoch) < 0);
    }

    @Test
    void higherReplicaIdBreaksEqualEpochPairTie() {
        ElectionCandidate lowerId = new ElectionCandidate(2, new EpochPair(2, 4));

        ElectionCandidate higherId = new ElectionCandidate(5, new EpochPair(2, 4));

        assertTrue(higherId.compareTo(lowerId) > 0);
        assertTrue(lowerId.compareTo(higherId) < 0);
    }

    @Test
    void equalCandidatesCompareAsEqual() {
        ElectionCandidate first = new ElectionCandidate(5, new EpochPair(2, 4));

        ElectionCandidate same = new ElectionCandidate(5, new EpochPair(2, 4));

        assertEquals(0, first.compareTo(same));
    }

    @Test
    void realUpdateBeatsCandidateWithNoObservedUpdate() {
        ElectionCandidate noUpdate = new ElectionCandidate(5);

        ElectionCandidate realUpdate = new ElectionCandidate(0, new EpochPair(0, 0));

        assertTrue(realUpdate.compareTo(noUpdate) > 0);
        assertTrue(noUpdate.compareTo(realUpdate) < 0);
    }

    @Test
    void noUpdateCandidatesUseReplicaIdAsTieBreaker() {
        ElectionCandidate lowerId = new ElectionCandidate(2);

        ElectionCandidate higherId = new ElectionCandidate(5);

        assertTrue(higherId.compareTo(lowerId) > 0);
        assertTrue(lowerId.compareTo(higherId) < 0);
    }

    @Test
    void ringNavigationHandlesWraparoundAndSkippedReplicas() {
        ElectionTransaction.RingNavigation ring = new ElectionTransaction.RingNavigation(List.of(0, 2, 3, 5));

        assertEquals(3, ring.nextReplicaId(2, Set.of()));
        assertEquals(0, ring.nextReplicaId(5, Set.of()));
        assertEquals(5, ring.nextReplicaId(2, Set.of(3)));
        assertEquals(3, ring.nextReplicaId(5, Set.of(0, 2)));
    }

    @Test
    void ringNavigationFailsWhenNoReplicaIsAvailable() {
        ElectionTransaction.RingNavigation ring = new ElectionTransaction.RingNavigation(List.of(0, 2, 3, 5));

        assertThrows(IllegalStateException.class, () -> ring.nextReplicaId(2, Set.of(0, 3, 5)));
    }

    @Test
    void electionMessageCopiesAndProtectsCandidateList() {
        ElectionCandidate candidate = new ElectionCandidate(3, new EpochPair(2, 4));

        List<ElectionCandidate> originalCandidates = new ArrayList<>(List.of(candidate));

        ElectionTransaction.ElectionMsg message =
                new ElectionTransaction.ElectionMsg(null, new EpochPair(2, 4), null, 1, originalCandidates);

        originalCandidates.clear();

        assertEquals(1, message.candidates.size());
        assertSame(candidate, message.candidates.get(0));

        assertThrows(UnsupportedOperationException.class, () -> message.candidates.add(candidate));
    }

    @Test
    void electionAckPreservesTransactionMetadata() {
        TransactionId transactionId = new TransactionId(null, 7);
        EpochPair epochPair = new EpochPair(2, 4);

        ElectionTransaction.ElectionAckMsg ack = new ElectionTransaction.ElectionAckMsg(transactionId, epochPair, null);

        assertSame(transactionId, ack.transactionId);
        assertSame(epochPair, ack.epochPair);
    }

    @Test
    void electionAckTimeoutPreservesTargetAndAttemptVersion() {
        TransactionId transactionId = new TransactionId(null, 8);
        EpochPair epochPair = new EpochPair(2, 4);

        ElectionTransaction.ElectionAckTimeoutMsg timeout =
                new ElectionTransaction.ElectionAckTimeoutMsg(transactionId, epochPair, null, 5, 2L);

        assertSame(transactionId, timeout.transactionId);
        assertSame(epochPair, timeout.epochPair);
        assertEquals(5, timeout.expectedTargetId);
        assertEquals(2L, timeout.attemptVersion);
    }

    @Test
    void synchronizationMessageProtectsPositionsSnapshot() {
        int[] originalPositions = {1, 2, 3};

        ElectionTransaction.SynchronizationMsg message = new ElectionTransaction.SynchronizationMsg(
                null, new EpochPair(3, 0), null, 1, 5, new EpochPair(3, 0), originalPositions);

        originalPositions[0] = 99;
        int[] firstRead = message.getPositions();
        firstRead[1] = 88;

        assertArrayEquals(new int[] {1, 2, 3}, message.getPositions());
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

            system.probes.get(1).expectNoMessage(Duration.ofMillis(250));
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
            system.probes.get(1).expectNoMessage(Duration.ofMillis(250));

            SynchronizationMsg validSynchronization = new SynchronizationMsg(
                    synchronizationId,
                    newEpochPair,
                    system.actors.get(2),
                    0,
                    2,
                    newEpochPair,
                    new int[AbstractReplica.POSITIONS_LIST_LENGTH]);
            follower.tell(validSynchronization, Actor.noSender());

            CoordinatorElected elected =
                    system.probes.get(1).expectMsgClass(Duration.ofSeconds(1), CoordinatorElected.class);
            assertEquals(2, elected.newCoordinatorId);
            assertEquals(1, elected.replicaId);
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
            system.probes.get(1).expectNoMessage(Duration.ofMillis(250));

            EpochPair newEpochPair = new EpochPair(1, 0);
            SynchronizationMsg validSynchronization = new SynchronizationMsg(
                    new TransactionId(system.actors.get(2), -1),
                    newEpochPair,
                    system.actors.get(2),
                    0,
                    2,
                    newEpochPair,
                    new int[AbstractReplica.POSITIONS_LIST_LENGTH]);
            follower.tell(validSynchronization, Actor.noSender());

            CoordinatorElected elected =
                    system.probes.get(1).expectMsgClass(Duration.ofSeconds(1), CoordinatorElected.class);
            assertEquals(2, elected.newCoordinatorId);
        } finally {
            system.system.terminate();
        }
    }
}
