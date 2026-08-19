package it.unitn.ds;

import java.io.Serializable;

public class EpochPair implements Comparable<EpochPair>, Serializable {
    protected final int epoch;
    protected final int sequence;

    public EpochPair(int epoch, int sequence) {
        this.epoch = epoch;
        this.sequence = sequence;
    }

    @Override
    public int compareTo(EpochPair other) {
        if (this.epoch != other.epoch) {
            return Integer.compare(this.epoch, other.epoch);
        } else {
            return Integer.compare(this.sequence, other.sequence);
        }
    }

    @Override
    public String toString() {
        return "<" + epoch + ", " + sequence + ">";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        EpochPair other = (EpochPair) obj;
        return this.epoch == other.epoch && this.sequence == other.sequence;
    }

    @Override
    public int hashCode() {
        int result = Integer.hashCode(epoch);
        result = 31 * result + Integer.hashCode(sequence);
        return result;
    }

    public int getEpoch() {
        return epoch;
    }

    public int getSequence() {
        return sequence;
    }
}
