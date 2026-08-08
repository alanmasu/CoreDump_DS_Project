package it.unitn.ds.regression;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import it.unitn.ds.EpochPair;

public class TestEpochPair {
    
    @Test
    void testEpochPairComparison() {
        EpochPair pair1 = new EpochPair(1, 1);
        EpochPair pair2 = new EpochPair(1, 2);
        EpochPair pair3 = new EpochPair(1, 1); 


        assertTrue(pair1.compareTo(pair2) < 0);
        assertTrue(pair2.compareTo(pair1) > 0);
        assertTrue(pair1.compareTo(pair1) == 0);
        assertTrue(pair2.compareTo(pair2) == 0);
        assertTrue(pair3.compareTo(pair1) == 0);
        assertTrue(pair1.compareTo(pair3) == 0);
        assertTrue(pair1.equals(pair1));
        assertFalse(pair1.equals(pair2));
        assertTrue(pair3.equals(pair1));

        assertTrue(pair1.hashCode() == pair3.hashCode());
        assertFalse(pair1.hashCode() == pair2.hashCode());

    }
    
}
