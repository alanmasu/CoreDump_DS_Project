package it.unitn.ds.regression;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.unitn.ds.EpochPair;
import org.junit.jupiter.api.Test;

class TestEpochPair {

    @Test
    void testEpochPairComparison() {
        EpochPair first = new EpochPair(1, 1);
        EpochPair second = new EpochPair(1, 2);

        assertTrue(first.compareTo(second) < 0, "<1,1> should sort before <1,2>");
        assertTrue(second.compareTo(first) > 0, "compareTo should be antisymmetric");
    }

    @Test
    void testEpochTakesPrecedenceOverSequence() {
        EpochPair earlierEpoch = new EpochPair(1, 100);
        EpochPair laterEpoch = new EpochPair(2, 0);

        assertTrue(earlierEpoch.compareTo(laterEpoch) < 0, "a lower epoch wins over a higher sequence number");
    }

    @Test
    void testEqualityAndHashCode() {
        EpochPair first = new EpochPair(1, 1);
        EpochPair sameValue = new EpochPair(1, 1);
        EpochPair differentValue = new EpochPair(1, 2);

        assertEquals(first, sameValue, "pairs with the same epoch and sequence should be equal");
        assertNotEquals(first, differentValue, "a different sequence number should break equality");
        assertEquals(first.hashCode(), sameValue.hashCode(), "equal pairs must share a hashCode");
    }
}
