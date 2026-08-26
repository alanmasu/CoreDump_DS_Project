package it.unitn.ds;

import java.io.Serializable;
import java.util.Objects;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Map;
import java.util.Arrays;

import akka.actor.ActorRef;

public final class ElectionTransaction extends Transaction {
    
    private enum State {
        NEW,
        PARTICIPATING,
        ELECTED,
        SYNCHRONIZING,
        DONE
    }

    private State state;

    private final int failedCoordinatorId;
    private final Map<Integer, ActorRef> replicaRefs;
    private final RingNavigation ringNavigation;
    private final boolean localInitiator;
    private final Set<Integer> unavailableReplicaIds;
    private int pendingTargetId;
    private long attemptVersion;
    private ElectionMsg pendingMessage;

    /**
     * Keeps the original constructor available for code that only creates an
     * election transaction to inspect its initial lifecycle state.
     *
     * A fully operational election must use the constructor that also
     * receives the failed coordinator and the replica group.
     */
    public ElectionTransaction(
            TransactionId id,
            DistributedActor owner,
            EpochPair startEpochPair) {
        super(id, owner, startEpochPair);
        this.failedCoordinatorId = -1;
        this.replicaRefs = Map.of();
        this.ringNavigation = null;
        this.localInitiator = false;
        this.unavailableReplicaIds = new HashSet<>();
        this.pendingTargetId = -1;
        this.state = State.NEW;
    }

    public ElectionTransaction(TransactionId id, DistributedActor owner, EpochPair startEpochPair,
                                int failedCoordinatorId, Map<Integer, ActorRef> replicaRefs)
    {
        this(id, owner, startEpochPair, failedCoordinatorId, replicaRefs, true);
    }

    public ElectionTransaction(TransactionId id, DistributedActor owner, EpochPair startEpochPair,
                                int failedCoordinatorId, Map<Integer, ActorRef> replicaRefs,
                                boolean localInitiator)
    {
        super(id, owner, startEpochPair);
        this.failedCoordinatorId = failedCoordinatorId;
        this.replicaRefs = Map.copyOf(
            Objects.requireNonNull(
                replicaRefs,
                "replicaRefs must not be null")
        );

        this.ringNavigation = new RingNavigation(
            List.copyOf(this.replicaRefs.keySet()));
        this.localInitiator = localInitiator;
        this.unavailableReplicaIds = new HashSet<>();
        this.unavailableReplicaIds.add(failedCoordinatorId);
        this.pendingTargetId = -1;
        this.state = State.NEW;
    }
    /**
     * 
     * Network message carrying the election token around the ring.
     * 
     * The candidate list is copied when the message is create, so later changes to the caller's list
     * cannot modify a message already in transit.
     */
    public static final class ElectionMsg extends Msg {
        
        public final int failedCoordinatorId;
        public final List<ElectionCandidate> candidates;

        public ElectionMsg(
            TransactionId transactionId,
            EpochPair epochPair,
            ActorRef sender,
            int failedCoordinatorId,
            List<ElectionCandidate> candidates
        ) 
        {
            super(transactionId, epochPair, sender);
            this.failedCoordinatorId = failedCoordinatorId;
            this.candidates = List.copyOf(
                Objects.requireNonNull(
                    candidates,
                    "candidates must not be null")
            );
        }

    }

    /**
     * Network message acknowledging receipt of an election token.
     * 
     * The sender is the replica that received the election message.
     * The transaction ID identifies which election token is being acknowledged.
     */
    public static final class ElectionAckMsg extends Msg {

        public ElectionAckMsg(TransactionId transactionId, EpochPair epochPair, ActorRef sender) {
            super(transactionId, epochPair, sender);
        }
    }

    /**
     * Local scheduler message used when a replica is waiting for an ACK.
     * 
     * This message never crosses the network. It is delivered to the owning replica's mailbox.
     */
    public static final class ElectionAckTimeoutMsg extends Msg {

        // The replica whose ACK we are waiting for
        public final int expectedTargetId;
        // Identifies the current timeout attempt
        public final long attemptVersion;

        public ElectionAckTimeoutMsg(
            TransactionId transactionId,
            EpochPair epochPair,
            ActorRef sender,
            int expectedTargetId,
            long attemptVersion)
        {
            super(transactionId, epochPair, sender);

            this.expectedTargetId = expectedTargetId;
            this.attemptVersion = attemptVersion;
        }
    }

    /**
     * Local scheduler message used to give the replicas a deterministic
     * opportunity to start the election in ring order.
     */
    public static final class ElectionStartMsg extends Msg {

        public final int failedCoordinatorId;

        public ElectionStartMsg(
                TransactionId transactionId,
                EpochPair epochPair,
                ActorRef sender,
                int failedCoordinatorId) {
            super(transactionId, epochPair, sender);
            this.failedCoordinatorId = failedCoordinatorId;
        }
    }

    /**
     * Network message telling a competing election transaction to stop.
     */
    public static final class ElectionRejectMsg extends Msg {

        public ElectionRejectMsg(
                TransactionId transactionId,
                EpochPair epochPair,
                ActorRef sender) {
            super(transactionId, epochPair, sender);
        }
    }

    /**
     * Network message announcing the elected coordinator and carrying the
     * authoritative replica snapshot.
     */
    public static final class SynchronizationMsg extends Msg {

        public final int failedCoordinatorId;
        public final int newCoordinatorId;
        public final EpochPair newEpochPair;
        private final int[] positions;

        public SynchronizationMsg(
                TransactionId transactionId,
                EpochPair epochPair,
                ActorRef sender,
                int failedCoordinatorId,
                int newCoordinatorId,
                EpochPair newEpochPair,
                int[] positions) {
            super(transactionId, epochPair, sender);
            this.failedCoordinatorId = failedCoordinatorId;
            this.newCoordinatorId = newCoordinatorId;
            this.newEpochPair = Objects.requireNonNull(
                    newEpochPair,
                    "newEpochPair must not be null");
            this.positions = Arrays.copyOf(
                    Objects.requireNonNull(
                            positions,
                            "positions must not be null"),
                    positions.length);
        }

        public int[] getPositions() {
            return Arrays.copyOf(positions, positions.length);
        }
    }

    // TODO: Need javadoc here
    public static final class ElectionCandidate implements Comparable<ElectionCandidate>, Serializable {
        
        private static final long serialVersionUID = 1L;
        private final int replicaId;
        private final boolean hasObservedUpdate;
        private final EpochPair latestObservedEpochPair;

        public ElectionCandidate(int replicaId, EpochPair latestObservedEpochPair) {

            this.replicaId = replicaId;
            this.hasObservedUpdate = true;
            this.latestObservedEpochPair = Objects.requireNonNull(
                latestObservedEpochPair,
                "latestObservedEpochPair must not be null.");
        }


        public ElectionCandidate(int replicaId) {
            this.replicaId = replicaId;
            this.hasObservedUpdate = false;
            this.latestObservedEpochPair = null;
        }

        public boolean hasObservedUpdate() {
            return hasObservedUpdate;
        }

        public int getReplicaId() {
            return replicaId;
        }

        public EpochPair getLatestObservedEpochPair() {
            return latestObservedEpochPair;
        }

        @Override
        public int compareTo(ElectionCandidate other) {
            Objects.requireNonNull(other, "other candidate must not be null");

            // If just one candidate has observed update, return which one did.
            if (hasObservedUpdate != other.hasObservedUpdate) {
                return hasObservedUpdate ? 1 : -1;
            }

            if (hasObservedUpdate) {
                int epochPairComparison = latestObservedEpochPair.compareTo(other.latestObservedEpochPair);

                if (epochPairComparison != 0) {
                    return epochPairComparison;
                }
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
            
            // We need a for loop here since if an element is unavailable, we need to increment
            // the offset and go look at the next element.
            for (int offset = 1; offset < ringReplicaIds.size(); offset++) {
                // Pick next candidate index
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

    public int getFailedCoordinatorId() {
        return failedCoordinatorId;
    }

    private Replica getReplicaOwner() {
        if (!(owner instanceof Replica)) {
            throw new IllegalStateException(
                    "ElectionTransaction owner must be a Replica");
        }
        return (Replica) owner;
    }

    private ElectionCandidate localCandidate(Replica replica) {
        EpochPair localEpochPair = replica.getEpochPair();
        if (localEpochPair == null) {
            return new ElectionCandidate(replica.getId());
        }
        return new ElectionCandidate(replica.getId(), localEpochPair);
    }

    private boolean containsLocalCandidate(
            List<ElectionCandidate> candidates,
            int replicaId) {
        for (ElectionCandidate candidate : candidates) {
            if (candidate.getReplicaId() == replicaId) {
                return true;
            }
        }
        return false;
    }

    private ElectionCandidate bestCandidate(List<ElectionCandidate> candidates) {
        if (candidates.isEmpty()) {
            throw new IllegalArgumentException(
                    "Election candidate list must not be empty");
        }

        ElectionCandidate best = candidates.get(0);
        for (int i = 1; i < candidates.size(); i++) {
            ElectionCandidate candidate = candidates.get(i);
            if (candidate.compareTo(best) > 0) {
                best = candidate;
            }
        }
        return best;
    }

    private List<ElectionCandidate> withoutCandidate(
            List<ElectionCandidate> candidates,
            int replicaId) {
        List<ElectionCandidate> filtered = new ArrayList<>();
        for (ElectionCandidate candidate : candidates) {
            if (candidate.getReplicaId() != replicaId) {
                filtered.add(candidate);
            }
        }
        return filtered;
    }

    private Integer replicaIdFor(ActorRef actorRef) {
        if (actorRef == null) {
            return null;
        }
        for (Map.Entry<Integer, ActorRef> entry : replicaRefs.entrySet()) {
            if (entry.getValue().equals(actorRef)) {
                return entry.getKey();
            }
        }
        return null;
    }

    private void cancelPendingTimeout() {
        if (timeout != null) {
            timeout.cancel();
            timeout = null;
        }
    }

    private long electionAckTimeoutMillis(Replica replica) {
        // The receiver sends the ACK only after forwarding the token. The
        // sender therefore needs time for the token and the ACK to make a
        // complete round trip through the network.
        return 2L * replica.getMaxLatencyPlusTolerance();
    }

    private ElectionMsg createOutgoingMessage(
            Replica replica,
            List<ElectionCandidate> candidates) {
        return new ElectionMsg(
                getId(),
                startEpochPair,
                replica.getSelf(),
                failedCoordinatorId,
                candidates);
    }

    private boolean sendToTarget(
            List<ElectionCandidate> candidates,
            int targetId) {
        Replica replica = getReplicaOwner();
        ActorRef target = replicaRefs.get(targetId);

        if (target == null
                || target.equals(replica.getSelf())
                || unavailableReplicaIds.contains(targetId)) {
            return false;
        }

        ElectionMsg outgoing = createOutgoingMessage(replica, candidates);
        pendingTargetId = targetId;
        pendingMessage = outgoing;
        attemptVersion++;

        replica.unicast(outgoing, target);
        timeout = replica.scheduleToItself(
                electionAckTimeoutMillis(replica),
                new ElectionAckTimeoutMsg(
                        getId(),
                        startEpochPair,
                        replica.getSelf(),
                        pendingTargetId,
                        attemptVersion));
        return true;
    }

    private boolean forwardToNext(List<ElectionCandidate> candidates) {
        Replica replica = getReplicaOwner();
        try {
            int nextReplicaId = ringNavigation.nextReplicaId(
                    replica.getId(),
                    unavailableReplicaIds);
            return sendToTarget(candidates, nextReplicaId);
        } catch (IllegalStateException noAvailableReplica) {
            return false;
        }
    }

    private void acknowledge(
            ActorRef previousSender,
            EpochPair messageEpochPair) {
        if (previousSender == null) {
            return;
        }

        Replica replica = getReplicaOwner();
        replica.unicast(
                new ElectionAckMsg(
                        getId(),
                        messageEpochPair,
                        replica.getSelf()),
                previousSender);
    }

    private void finishWithCandidateList(
            List<ElectionCandidate> candidates) {
        Replica replica = getReplicaOwner();
        ElectionCandidate winner = bestCandidate(candidates);

        if (winner.getReplicaId() == replica.getId()) {
            state = State.ELECTED;
            replica.completeElectionAsWinner(
                    failedCoordinatorId,
                    getId(),
                    candidates);
            return;
        }

        if (!sendToTarget(candidates, winner.getReplicaId())) {
            unavailableReplicaIds.add(winner.getReplicaId());
            List<ElectionCandidate> remainingCandidates = withoutCandidate(
                    candidates,
                    winner.getReplicaId());
            if (remainingCandidates.isEmpty()
                    || !forwardToNext(remainingCandidates)) {
                if (!remainingCandidates.isEmpty()) {
                    finishWithCandidateList(remainingCandidates);
                } else {
                    state = State.DONE;
                }
            }
        }
    }

    private void handleElectionMessage(ElectionMsg message) {
        if (message.failedCoordinatorId != failedCoordinatorId) {
            return;
        }

        Replica replica = getReplicaOwner();
        List<ElectionCandidate> candidates = message.candidates;

        if (containsLocalCandidate(candidates, replica.getId())) {
            ElectionCandidate winner = bestCandidate(candidates);
            if (winner.getReplicaId() == replica.getId()) {
                acknowledge(message.sender, message.epochPair);
                finishWithCandidateList(candidates);
            } else {
                boolean forwarded = sendToTarget(
                        candidates,
                        winner.getReplicaId());
                if (!forwarded) {
                    unavailableReplicaIds.add(winner.getReplicaId());
                    List<ElectionCandidate> remainingCandidates =
                            withoutCandidate(
                                    candidates,
                                    winner.getReplicaId());
                    forwarded = !remainingCandidates.isEmpty()
                            && forwardToNext(remainingCandidates);
                    if (!forwarded && !remainingCandidates.isEmpty()) {
                        finishWithCandidateList(remainingCandidates);
                    }
                }
                acknowledge(message.sender, message.epochPair);
            }
            return;
        }

        List<ElectionCandidate> updatedCandidates =
                new ArrayList<>(candidates);
        updatedCandidates.add(localCandidate(replica));

        boolean forwarded = forwardToNext(updatedCandidates);
        acknowledge(message.sender, message.epochPair);

        if (!forwarded) {
            finishWithCandidateList(updatedCandidates);
        }
    }

    private void handleAcknowledgement(
            ElectionAckMsg acknowledgement) {
        if (pendingMessage == null) {
            return;
        }

        Integer acknowledgementSender =
                replicaIdFor(acknowledgement.sender);
        if (acknowledgementSender == null
                || acknowledgementSender != pendingTargetId) {
            return;
        }

        cancelPendingTimeout();
        pendingMessage = null;
        pendingTargetId = -1;
    }

    private void handleRejection() {
        cancelPendingTimeout();
        pendingMessage = null;
        pendingTargetId = -1;
        state = State.DONE;

        Replica replica = getReplicaOwner();
        replica.onElectionTransactionCancelled(
                failedCoordinatorId,
                getId());
        replica.onTransactionComplete(this);
    }

    private void handleTimeout(
            ElectionAckTimeoutMsg timeoutMessage) {
        if (pendingMessage == null
                || timeoutMessage.attemptVersion != attemptVersion
                || timeoutMessage.expectedTargetId != pendingTargetId) {
            return;
        }

        ElectionMsg messageToForward = pendingMessage;
        pendingMessage = null;
        pendingTargetId = -1;
        timeout = null;
        unavailableReplicaIds.add(timeoutMessage.expectedTargetId);

        List<ElectionCandidate> remainingCandidates = withoutCandidate(
                messageToForward.candidates,
                timeoutMessage.expectedTargetId);

        if (remainingCandidates.isEmpty()) {
            state = State.DONE;
            return;
        }

        if (!forwardToNext(remainingCandidates)) {
            finishWithCandidateList(remainingCandidates);
        }
    }



    @Override
    public String getState() {
        return state.toString();
    }

    @Override
    public void computeState(Msg msg) {
        if (msg instanceof ElectionMsg) {
            handleElectionMessage((ElectionMsg) msg);
            return;
        }

        if (msg instanceof ElectionAckMsg) {
            handleAcknowledgement((ElectionAckMsg) msg);
            return;
        }

        if (msg instanceof ElectionRejectMsg) {
            handleRejection();
            return;
        }

        if (msg instanceof ElectionAckTimeoutMsg) {
            handleTimeout((ElectionAckTimeoutMsg) msg);
            return;
        }

        throw new IllegalArgumentException(
                "Unsupported election message: "
                        + msg.getClass().getSimpleName());
    }

    @Override
    public void start() {
        if (state != State.NEW) {
            return;
        }

        state = State.PARTICIPATING;

        if (!localInitiator || ringNavigation == null) {
            return;
        }

        Replica replica = getReplicaOwner();

        ElectionMsg initialMessage = createOutgoingMessage(
                replica,
                List.of(localCandidate(replica)));

        if (!forwardToNext(initialMessage.candidates)) {
            finishWithCandidateList(initialMessage.candidates);
        }
    }
}
