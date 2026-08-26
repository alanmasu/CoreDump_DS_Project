package it.unitn.ds;

import java.io.Serializable;
import java.util.Objects;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public final class ElectionTransaction extends Transaction {
    
    private enum State {
        NEW,
        PARTICIPATING,
        ELECTED,
        SYNCHRONIZING,
        DONE
    }

    private State state;

    public static final class ElectionCandidate implements Comparable<ElectionCandidate>, Serializable {
        
        private static final long serialVersionUID = 1L;
        private final int replicaId;
        private final EpochPair latestObservedEpochPair;

        public ElectionCandidate(int replicaId, EpochPair latestObserbedEpochPair) {
            this.replicaId = replicaId;
            this.latestObservedEpochPair = Objects.requireNonNull(
                latestObserbedEpochPair,
                "latestObservedEpochPair must not be null");
        }

        public int getReplicaId() {
            return replicaId;
        }

        public EpochPair getLatestObservEpochPair() {
            return latestObservedEpochPair;
        }

        @Override
        public int compareTo(ElectionCandidate other) {
            Objects.requireNonNull(other, "other candidate must not be null");

            int epochPairComparison = latestObservedEpochPair.compareTo(other.latestObservedEpochPair);

            if (epochPairComparison != 0) {
                return epochPairComparison;
            }

            return Integer.compare(replicaId, other.replicaId);
        }
    }


    public static final class RingNavigation {
        
        private final List<Integer> ringReplicaIds;

        public RingNavigation(List<Integer> replicaIds) {
            Objects.requireNonNull(replicaIds, "replicaIds must not be null");

            if (replicaIds.isEmpty()) {
                throw new IllegalArgumentException(
                    "replicaIds must not be empty");
            }

            List<Integer> sortedIds = new ArrayList<>(replicaIds);

            for (Integer replicaId : sortedIds) {
                Objects.requireNonNull(
                    replicaId,
                    "replica IDs must not contain null");
            }

            Collections.sort(sortedIds);

            for (int i = 1; i < sortedIds.size(); i++) {
                if (sortedIds.get(i).equals(sortedIds.get(i - 1))) {
                    throw new IllegalArgumentException(
                        "replica IDs must be unique");
                }
            }

            this.ringReplicaIds = Collections.unmodifiableList(sortedIds);
        }

        public int nextReplicaId(int currentReplicaId, Set<Integer> unavailableReplicaIds) {
            Objects.requireNonNull(
                unavailableReplicaIds,
                "unavailableReplicaIds must not be null");

            int currentIndex = ringReplicaIds.indexOf(currentReplicaId);
            if (currentIndex < 0) {
                throw new IllegalArgumentException(
                    "current replica is not part of the ring");
            }
            
            for (int offset = 1; offset < ringReplicaIds.size(); offset++) {
                int candidateIndex = (currentIndex + offset) % ringReplicaIds.size();
                int candidateId = ringReplicaIds.get(candidateIndex);

                if (!unavailableReplicaIds.contains(candidateId)) {
                    return candidateId;
                }
            }

            throw new IllegalStateException(
                "no available replica exists in the ring");
        }
    }

    public ElectionTransaction(
        TransactionId id,
        DistributedActor owner,
        EpochPair startEpochPair) 
    {
        super(id, owner, startEpochPair);
        this.state = State.NEW;
    }

    @Override
    public String getState() {
        return state.toString();
    }

    @Override
    public void computeState(Msg msg) {
        // TODO
    }

    @Override
    public void start() {
        state = State.PARTICIPATING;
    }
}
