package it.unitn.ds.regression;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Set;

import it.unitn.ds.ElectionTransaction;
import it.unitn.ds.EpochPair;
import it.unitn.ds.ElectionTransaction.ElectionCandidate;

import org.junit.jupiter.api.Test;

public class TestElectionTransaction {

    @Test
    void newerSequenceWinsWhenEpochIsEqual() {
        ElectionCandidate older = 
            new ElectionCandidate(3, new EpochPair(2, 3));

        ElectionCandidate newer = 
            new ElectionCandidate(2, new EpochPair(2, 4));
        
        assertTrue(newer.compareTo(older) > 0);
        assertTrue(older.compareTo(newer) < 0);
    }

    @Test
    void newerEpochWinsOverHigherSequence() {
        ElectionCandidate olderEpoch = 
            new ElectionCandidate(9, new EpochPair(2, 99));
        
        ElectionCandidate newerEpoch =
            new ElectionCandidate(0, new EpochPair(3, 0));

        assertTrue(newerEpoch.compareTo(olderEpoch) > 0);
        assertTrue(olderEpoch.compareTo(newerEpoch) < 0);
    }


    @Test
    void higherReplicaIdBreaksEqualEpochPairTie() {
        ElectionCandidate lowerId =
            new ElectionCandidate(2, new EpochPair(2, 4));
        
        ElectionCandidate higherId =
            new ElectionCandidate(5, new EpochPair(2, 4));
        
        assertTrue(higherId.compareTo(lowerId) > 0);
        assertTrue(lowerId.compareTo(higherId) < 0);
    }

    @Test
    void equalCandidatesCompareAsEqual() {
        ElectionCandidate first =
                new ElectionCandidate(5, new EpochPair(2, 4));

        ElectionCandidate same =
                new ElectionCandidate(5, new EpochPair(2, 4));

        assertEquals(0, first.compareTo(same));
    }


    @Test
    void ringNavigationHandlesWraparoundAndSkippedReplicas() {
        ElectionTransaction.RingNavigation ring =
            new ElectionTransaction.RingNavigation(
                List.of(0, 2, 3, 5));
        
        assertEquals(3, ring.nextReplicaId(2, Set.of()));
        assertEquals(0, ring.nextReplicaId(5, Set.of()));
        assertEquals(5, ring.nextReplicaId(2, Set.of(3)));
        assertEquals(3, ring.nextReplicaId(5, Set.of(0, 2)));
    }

    @Test
    void ringNavigationFailsWhenNoReplicaIsAvailable() {
        ElectionTransaction.RingNavigation ring =
                new ElectionTransaction.RingNavigation(List.of(0, 2, 3, 5));

        assertThrows(
                IllegalStateException.class,
                () -> ring.nextReplicaId(
                        2,
                        Set.of(0, 3, 5)));
    }
}
