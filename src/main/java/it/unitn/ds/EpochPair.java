package it.unitn.ds;


public class EpochPair implements Comparable<EpochPair> {
    protected int epoch;
    protected int sequence;

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

    public int getEpoch() {
        return epoch;
    }
    public int getSequence() {
        return sequence;
    }

    public void setEpoch(int newEpoch) {
        this.epoch = newEpoch;
    }
    public void setSequence(int newSequence) {
        this.sequence = newSequence;
    }
}
