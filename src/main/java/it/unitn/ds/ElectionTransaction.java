package it.unitn.ds;

import akka.actor.ActorRef;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Coordinates a ring-based election when a replica detects that the current
 * coordinator has failed.
 *
 * <p>The transaction forwards an election token through the available
 * replicas, selects the best candidate, and lets the winning replica publish
 * the new coordinator and synchronized state.</p>
 */
@SuppressWarnings("PMD.NullAssignment")
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
     *
     * @param id transaction identifier
     * @param owner replica that owns this transaction
     * @param startEpochPair epoch pair observed when the transaction was created
     */
    public ElectionTransaction(TransactionId id, DistributedActor owner, EpochPair startEpochPair) {
        super(id, owner, startEpochPair);
        this.failedCoordinatorId = -1;
        this.replicaRefs = Map.of();
        this.ringNavigation = null;
        this.localInitiator = false;
        this.unavailableReplicaIds = new HashSet<>();
        this.pendingTargetId = -1;
        this.state = State.NEW;
    }

    /**
     * Creates an election transaction that participates in the election ring.
     *
     * @param id transaction identifier shared by the election token
     * @param owner replica that owns this transaction
     * @param startEpochPair epoch pair observed when the election started
     * @param failedCoordinatorId identifier of the failed coordinator
     * @param replicaRefs all replicas that can participate in the ring
     */
    public ElectionTransaction(
            TransactionId id,
            DistributedActor owner,
            EpochPair startEpochPair,
            int failedCoordinatorId,
            Map<Integer, ActorRef> replicaRefs) {
        this(id, owner, startEpochPair, failedCoordinatorId, replicaRefs, true);
    }

    /**
     * Creates an election transaction with an explicit initiator flag.
     *
     * @param id transaction identifier shared by the election token
     * @param owner replica that owns this transaction
     * @param startEpochPair epoch pair observed when the election started
     * @param failedCoordinatorId identifier of the failed coordinator
     * @param replicaRefs all replicas that can participate in the ring
     * @param localInitiator whether this replica should send the first token
     */
    public ElectionTransaction(
            TransactionId id,
            DistributedActor owner,
            EpochPair startEpochPair,
            int failedCoordinatorId,
            Map<Integer, ActorRef> replicaRefs,
            boolean localInitiator) {
        super(id, owner, startEpochPair);
        this.failedCoordinatorId = failedCoordinatorId;
        this.replicaRefs = Map.copyOf(Objects.requireNonNull(replicaRefs, "replicaRefs must not be null"));

        this.ringNavigation = new RingNavigation(List.copyOf(this.replicaRefs.keySet()));
        this.localInitiator = localInitiator;
        this.unavailableReplicaIds = new HashSet<>();
        this.unavailableReplicaIds.add(failedCoordinatorId);
        this.pendingTargetId = -1;
        this.state = State.NEW;
    }
    /**
     * Network message carrying the election token around the ring.
     *
     * <p>The candidate list is copied when the message is created, so later
     * changes to the caller's list cannot modify a message already in transit.</p>
     */
    public static final class ElectionMsg extends Msg {

        /** Identifier of the coordinator that triggered this election. */
        public final int failedCoordinatorId;
        /** Candidates collected by the election token so far. */
        public final List<ElectionCandidate> candidates;

        /**
         * Creates an election token message.
         *
         * @param transactionId identifier of the election transaction
         * @param epochPair epoch pair associated with the election
         * @param sender replica forwarding the token
         * @param failedCoordinatorId identifier of the failed coordinator
         * @param candidates candidates collected by the token
         */
        public ElectionMsg(
                TransactionId transactionId,
                EpochPair epochPair,
                ActorRef sender,
                int failedCoordinatorId,
                List<ElectionCandidate> candidates) {
            super(transactionId, epochPair, sender);
            this.failedCoordinatorId = failedCoordinatorId;
            this.candidates = List.copyOf(Objects.requireNonNull(candidates, "candidates must not be null"));
        }
    }

    /**
     * Network message acknowledging receipt of an election token.
     *
     * The sender is the replica that received the election message.
     * The transaction ID identifies which election token is being acknowledged.
     */
    public static final class ElectionAckMsg extends Msg {

        /**
         * Creates an acknowledgement for an election token.
         *
         * @param transactionId identifier of the election transaction
         * @param epochPair epoch pair associated with the election
         * @param sender replica sending the acknowledgement
         */
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

        /** The replica whose acknowledgement is expected. */
        public final int expectedTargetId;
        /** Version identifying the timeout attempt that expired. */
        public final long attemptVersion;

        /**
         * Creates a local election acknowledgement-timeout message.
         *
         * @param transactionId identifier of the election transaction
         * @param epochPair epoch pair associated with the election
         * @param sender replica waiting for the acknowledgement
         * @param expectedTargetId replica whose acknowledgement is expected
         * @param attemptVersion version of the timeout attempt
         */
        public ElectionAckTimeoutMsg(
                TransactionId transactionId,
                EpochPair epochPair,
                ActorRef sender,
                int expectedTargetId,
                long attemptVersion) {
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

        /** Identifier of the coordinator whose failure triggered the election. */
        public final int failedCoordinatorId;

        /**
         * Creates a local message that starts an election after its delay.
         *
         * @param transactionId identifier of the election transaction, if known
         * @param epochPair epoch pair observed when scheduling the election
         * @param sender replica scheduling the election
         * @param failedCoordinatorId identifier of the failed coordinator
         */
        public ElectionStartMsg(
                TransactionId transactionId, EpochPair epochPair, ActorRef sender, int failedCoordinatorId) {
            super(transactionId, epochPair, sender);
            this.failedCoordinatorId = failedCoordinatorId;
        }
    }

    /**
     * Network message telling a competing election transaction to stop.
     */
    public static final class ElectionRejectMsg extends Msg {

        /**
         * Creates a message rejecting a competing election token.
         *
         * @param transactionId identifier of the rejected election transaction
         * @param epochPair epoch pair associated with the election
         * @param sender replica rejecting the token
         */
        public ElectionRejectMsg(TransactionId transactionId, EpochPair epochPair, ActorRef sender) {
            super(transactionId, epochPair, sender);
        }
    }

    /**
     * Network message announcing the elected coordinator and carrying the
     * authoritative replica snapshot.
     */
    public static final class SynchronizationMsg extends Msg {

        /** Identifier of the coordinator whose failure triggered the election. */
        public final int failedCoordinatorId;
        /** Identifier of the newly elected coordinator. */
        public final int newCoordinatorId;
        /** Epoch pair assigned to the new coordinator term. */
        public final EpochPair newEpochPair;

        private final int[] positions;

        /**
         * Creates a synchronization message carrying the authoritative state.
         *
         * @param transactionId identifier of the election transaction
         * @param epochPair epoch pair associated with the election
         * @param sender newly elected coordinator
         * @param failedCoordinatorId identifier of the failed coordinator
         * @param newCoordinatorId identifier of the new coordinator
         * @param newEpochPair epoch pair assigned to the new coordinator term
         * @param positions authoritative positions snapshot
         */
        public SynchronizationMsg(
                TransactionId transactionId,
                EpochPair epochPair,
                ActorRef sender,
                int failedCoordinatorId,
                int newCoordinatorId,
                EpochPair newEpochPair,
                int... positions) {
            super(transactionId, epochPair, sender);
            this.failedCoordinatorId = failedCoordinatorId;
            this.newCoordinatorId = newCoordinatorId;
            this.newEpochPair = Objects.requireNonNull(newEpochPair, "newEpochPair must not be null");
            this.positions =
                    Arrays.copyOf(Objects.requireNonNull(positions, "positions must not be null"), positions.length);
        }

        /**
         * Returns a copy of the synchronized positions snapshot.
         *
         * @return copy of the authoritative positions
         */
        public int[] getPositions() {
            return Arrays.copyOf(positions, positions.length);
        }
    }

    /**
     * Candidate considered by the election ordering algorithm.
     *
     * <p>A candidate with an observed update is preferred over one without an
     * observed update. Among observed updates, the newest epoch pair wins;
     * replica ID breaks ties.</p>
     */
    public static final class ElectionCandidate implements Comparable<ElectionCandidate>, Serializable {

        private static final long serialVersionUID = 1L;
        private final int replicaId;
        private final boolean observedUpdate;
        private final EpochPair latestObservedEpochPair;

        /**
         * Creates a candidate with an observed update.
         *
         * @param replicaId identifier of the candidate replica
         * @param latestObservedEpochPair newest update epoch pair observed by the replica
         */
        public ElectionCandidate(int replicaId, EpochPair latestObservedEpochPair) {

            this.replicaId = replicaId;
            this.observedUpdate = true;
            this.latestObservedEpochPair =
                    Objects.requireNonNull(latestObservedEpochPair, "latestObservedEpochPair must not be null.");
        }
        /**
         * Creates a candidate without an observed update.
         *
         * @param replicaId identifier of the candidate replica
         */
        public ElectionCandidate(int replicaId) {
            this.replicaId = replicaId;
            this.observedUpdate = false;
            this.latestObservedEpochPair = null;
        }

        /**
         * Reports whether this candidate has an observed update.
         *
         * @return true when an epoch pair was observed
         */
        public boolean hasObservedUpdate() {
            return observedUpdate;
        }

        /**
         * Returns the candidate replica identifier.
         *
         * @return candidate replica identifier
         */
        public int getReplicaId() {
            return replicaId;
        }

        /**
         * Returns the newest update epoch pair observed by this candidate.
         *
         * @return observed epoch pair, or null when no update was observed
         */
        public EpochPair getLatestObservedEpochPair() {
            return latestObservedEpochPair;
        }

        /**
         * Compares candidates according to the election winner ordering.
         *
         * @param other candidate to compare with this candidate
         * @return a positive value when this candidate is preferred
         */
        @Override
        public int compareTo(ElectionCandidate other) {
            Objects.requireNonNull(other, "other candidate must not be null");

            // If just one candidate has observed update, return which one did.
            if (observedUpdate != other.observedUpdate) {
                return observedUpdate ? 1 : -1;
            }

            if (observedUpdate) {
                int epochPairComparison = latestObservedEpochPair.compareTo(other.latestObservedEpochPair);

                if (epochPairComparison != 0) {
                    return epochPairComparison;
                }
            }

            return Integer.compare(replicaId, other.replicaId);
        }

        /**
         * Compares this candidate with another candidate by value.
         *
         * @param other object to compare with this candidate
         * @return true when both candidates contain the same election data
         */
        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (!(other instanceof ElectionCandidate)) {
                return false;
            }
            ElectionCandidate candidate = (ElectionCandidate) other;
            return replicaId == candidate.replicaId
                    && observedUpdate == candidate.observedUpdate
                    && Objects.equals(latestObservedEpochPair, candidate.latestObservedEpochPair);
        }

        /**
         * Returns a hash code consistent with {@link #equals(Object)}.
         *
         * @return hash code for this candidate
         */
        @Override
        public int hashCode() {
            return Objects.hash(replicaId, observedUpdate, latestObservedEpochPair);
        }
    }

    /**
     * Provides sorted-ring traversal while skipping unavailable replicas.
     */
    public static final class RingNavigation {

        private final List<Integer> ringReplicaIds;

        /**
         * Creates ring navigation for the supplied replica identifiers.
         *
         * @param replicaIds identifiers in the replica group
         */
        public RingNavigation(List<Integer> replicaIds) {
            Objects.requireNonNull(replicaIds, "replicaIds must not be null");

            if (replicaIds.isEmpty()) {
                throw new IllegalArgumentException("replicaIds must not be empty");
            }

            List<Integer> sortedIds = new ArrayList<>(replicaIds);

            for (Integer replicaId : sortedIds) {
                Objects.requireNonNull(replicaId, "replica IDs must not contain null");
            }

            Collections.sort(sortedIds);

            for (int i = 1; i < sortedIds.size(); i++) {
                if (sortedIds.get(i).equals(sortedIds.get(i - 1))) {
                    throw new IllegalArgumentException("replica IDs must be unique");
                }
            }

            this.ringReplicaIds = Collections.unmodifiableList(sortedIds);
        }

        /**
         * Finds the next available replica after the current replica.
         *
         * @param currentReplicaId identifier of the current replica
         * @param unavailableReplicaIds identifiers that must be skipped
         * @return identifier of the next available replica, with wraparound
         * @throws IllegalArgumentException when the current replica is not in the ring
         * @throws IllegalStateException when no available replica exists
         */
        public int nextReplicaId(int currentReplicaId, Set<Integer> unavailableReplicaIds) {
            Objects.requireNonNull(unavailableReplicaIds, "unavailableReplicaIds must not be null");

            int currentIndex = ringReplicaIds.indexOf(currentReplicaId);
            if (currentIndex < 0) {
                throw new IllegalArgumentException("current replica is not part of the ring");
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

            throw new IllegalStateException("no available replica exists in the ring");
        }
    }

    /**
     * Returns the coordinator whose failure started this election.
     *
     * @return failed coordinator identifier
     */
    public int getFailedCoordinatorId() {
        return failedCoordinatorId;
    }

    private Replica getReplicaOwner() {
        if (!(owner instanceof Replica)) {
            throw new IllegalStateException("ElectionTransaction owner must be a Replica");
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

    private boolean containsLocalCandidate(List<ElectionCandidate> candidates, int replicaId) {
        for (ElectionCandidate candidate : candidates) {
            if (candidate.getReplicaId() == replicaId) {
                return true;
            }
        }
        return false;
    }

    private ElectionCandidate bestCandidate(List<ElectionCandidate> candidates) {
        if (candidates.isEmpty()) {
            throw new IllegalArgumentException("Election candidate list must not be empty");
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

    private List<ElectionCandidate> withoutCandidate(List<ElectionCandidate> candidates, int replicaId) {
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

    void enterSynchronizing() {
        if (state == State.PARTICIPATING || state == State.ELECTED) {
            state = State.SYNCHRONIZING;
        }
    }

    void complete() {
        cancelPendingTimeout();
        state = State.DONE;
    }

    private long electionAckTimeoutMillis(Replica replica) {
        // The receiver sends the ACK only after forwarding the token. The
        // sender therefore needs time for the token and the ACK to make a
        // complete round trip through the network.
        return 2L * replica.getMaxLatencyPlusTolerance();
    }

    private ElectionMsg createOutgoingMessage(Replica replica, List<ElectionCandidate> candidates) {
        return new ElectionMsg(getId(), startEpochPair, replica.getSelf(), failedCoordinatorId, candidates);
    }

    private boolean sendToTarget(List<ElectionCandidate> candidates, int targetId) {
        Replica replica = getReplicaOwner();
        ActorRef target = replicaRefs.get(targetId);

        if (target == null || target.equals(replica.getSelf()) || unavailableReplicaIds.contains(targetId)) {
            return false;
        }

        ElectionMsg outgoing = createOutgoingMessage(replica, candidates);
        pendingTargetId = targetId;
        pendingMessage = outgoing;
        attemptVersion++;

        replica.unicast(outgoing, target);
        timeout = replica.scheduleToItself(
                electionAckTimeoutMillis(replica),
                new ElectionAckTimeoutMsg(getId(), startEpochPair, replica.getSelf(), pendingTargetId, attemptVersion));
        return true;
    }

    private boolean forwardToNext(List<ElectionCandidate> candidates) {
        Replica replica = getReplicaOwner();
        try {
            int nextReplicaId = ringNavigation.nextReplicaId(replica.getId(), unavailableReplicaIds);
            return sendToTarget(candidates, nextReplicaId);
        } catch (IllegalStateException noAvailableReplica) {
            return false;
        }
    }

    private void acknowledge(ActorRef previousSender, EpochPair messageEpochPair) {
        if (previousSender == null) {
            return;
        }

        Replica replica = getReplicaOwner();
        replica.unicast(new ElectionAckMsg(getId(), messageEpochPair, replica.getSelf()), previousSender);
    }

    private void finishWithCandidateList(List<ElectionCandidate> candidates) {
        Replica replica = getReplicaOwner();
        ElectionCandidate winner = bestCandidate(candidates);

        if (winner.getReplicaId() == replica.getId()) {
            state = State.ELECTED;
            replica.completeElectionAsWinner(failedCoordinatorId, getId(), candidates);
            return;
        }

        if (!sendToTarget(candidates, winner.getReplicaId())) {
            unavailableReplicaIds.add(winner.getReplicaId());
            List<ElectionCandidate> remainingCandidates = withoutCandidate(candidates, winner.getReplicaId());
            if (remainingCandidates.isEmpty() || !forwardToNext(remainingCandidates)) {
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
                boolean forwarded = sendToTarget(candidates, winner.getReplicaId());
                if (!forwarded) {
                    unavailableReplicaIds.add(winner.getReplicaId());
                    List<ElectionCandidate> remainingCandidates = withoutCandidate(candidates, winner.getReplicaId());
                    forwarded = !remainingCandidates.isEmpty() && forwardToNext(remainingCandidates);
                    if (!forwarded && !remainingCandidates.isEmpty()) {
                        finishWithCandidateList(remainingCandidates);
                    }
                }
                acknowledge(message.sender, message.epochPair);
            }
            return;
        }

        List<ElectionCandidate> updatedCandidates = new ArrayList<>(candidates);
        updatedCandidates.add(localCandidate(replica));

        boolean forwarded = forwardToNext(updatedCandidates);
        acknowledge(message.sender, message.epochPair);

        if (!forwarded) {
            finishWithCandidateList(updatedCandidates);
        }
    }

    private void handleAcknowledgement(ElectionAckMsg acknowledgement) {
        if (pendingMessage == null) {
            return;
        }

        Integer acknowledgementSender = replicaIdFor(acknowledgement.sender);
        if (acknowledgementSender == null || acknowledgementSender != pendingTargetId) {
            return;
        }

        cancelPendingTimeout();
        pendingMessage = null;
        pendingTargetId = -1;
    }

    private void handleRejection() {
        complete();
        pendingMessage = null;
        pendingTargetId = -1;

        Replica replica = getReplicaOwner();
        replica.onElectionTransactionCancelled(failedCoordinatorId, getId());
        replica.onTransactionComplete(this);
    }

    private void handleTimeout(ElectionAckTimeoutMsg timeoutMessage) {
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

        List<ElectionCandidate> remainingCandidates =
                withoutCandidate(messageToForward.candidates, timeoutMessage.expectedTargetId);

        if (remainingCandidates.isEmpty()) {
            complete();
            return;
        }

        if (!forwardToNext(remainingCandidates)) {
            finishWithCandidateList(remainingCandidates);
        }
    }

    /**
     * Returns the current election state.
     *
     * @return current state name
     */
    @Override
    public String getState() {
        return state.toString();
    }

    /**
     * Processes one message belonging to this election transaction.
     *
     * @param msg election message to process
     * @throws IllegalArgumentException when the message type is unsupported
     */
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
                "Unsupported election message: " + msg.getClass().getSimpleName());
    }

    /**
     * Starts the election token when this transaction is the local initiator.
     *
     * <p>Non-initiating transactions wait for the first token received from
     * another replica.</p>
     */
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

        ElectionMsg initialMessage = createOutgoingMessage(replica, List.of(localCandidate(replica)));

        if (!forwardToNext(initialMessage.candidates)) {
            finishWithCandidateList(initialMessage.candidates);
        }
    }
}
