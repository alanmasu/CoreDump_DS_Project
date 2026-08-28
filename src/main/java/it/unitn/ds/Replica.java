package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.ds.HeartbeatTransaction.HeartbeatMsg;
import it.unitn.ds.HeartbeatTransaction.WatchdogExpiredMsg;
import it.unitn.ds.ProbeTransaction.ProbeMsg;
import it.unitn.ds.ReadTransaction.ReadMsg;
import it.unitn.ds.ReadTransaction.ReadResultMsg;
import it.unitn.ds.Transaction.TransactionId;
import it.unitn.ds.UpdateTransaction.UpdateAckMsg;
import it.unitn.ds.UpdateTransaction.UpdateMsg;
import it.unitn.ds.UpdateTransaction.UpdateTimeoutMsg;
import it.unitn.ds.UpdateTransaction.WriteOkMsg;
import it.unitn.ds.UpdateTransaction.WriteOkTimeoutMsg;
import it.unitn.ds.WriteTransaction.WriteFinishMsg;
import it.unitn.ds.WriteTransaction.WriteMsg;
import java.util.HashMap;
import it.unitn.ds.ElectionTransaction.ElectionMsg;
import it.unitn.ds.ElectionTransaction.ElectionStartMsg;
import it.unitn.ds.ElectionTransaction.SynchronizationMsg;
import java.util.LinkedList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class Replica extends AbstractReplica implements DistributedActor {
    private static final int INITIAL_HEARTBEAT_TRANSACTION_SEQUENCE = 0;
    private Map<Integer, ActorRef> groupOfReplicas;
    private Map<EpochPair, UpdateTransaction> updateHistory;
    private List<Transaction> activeTransactions;
    private int transactionCounter;
    private int updateSequenceEpoch;
    private int nextUpdateSequence;
    private int electionTransactionCounter;
    private EpochPair epochPair;
    private int positions[];
    private int coordinatorID;
    private final Set<Integer> startedElectionCoordinators;
    private final Set<Integer> scheduledElectionCoordinators;
    private final Set<Integer> completedElectionCoordinators;
    private final Map<Integer, TransactionId> electionTransactionIds;

    ///////////// For crashing ////////////
    private enum CrashStatus {
        NONE,
        PENDING,
        CRASHED
    };

    private CrashStatus replicaStatus;
    private int crashCount;
    private AbstractReplica.Crash pendingCrash;
    ///////////////////////////////////////

    // Manages heartbeat sending or coordinator monitoring for this Replica
    private HeartbeatTransaction heartbeatTransaction;

    public Replica(int id) {
        this(
                id,
                AbstractReplica.MIN_LATENCY,
                AbstractReplica.MAX_LATENCY,
                AbstractReplica.COORDINATOR_BEAT_INTERVAL,
                Optional.empty());
    }

    public Replica(int id, int minLatency, int maxLatency, int coordinatorBeatInterval, Optional<ActorRef> listener) {
        super(id, minLatency, maxLatency, coordinatorBeatInterval, listener);
        positions = new int[AbstractReplica.POSITIONS_LIST_LENGTH];
        // pendingCrash is left at its default null: no crash has been requested yet.
        this.replicaStatus = CrashStatus.NONE;
        this.crashCount = 0;
        this.activeTransactions = new LinkedList<>();
        this.transactionCounter = 0;
        this.updateHistory = new HashMap<>();
        this.epochPair = new EpochPair(0, 0);
        this.updateSequenceEpoch = this.epochPair.getEpoch();
        this.nextUpdateSequence = this.epochPair.getSequence() + 1;
        // Negative sequence numbers are reserved for election transactions.
        // Heartbeat transactions use sequence 0, while ordinary transaction IDs
        // obtained through getNextTransactionId() remain non-negative.
        this.electionTransactionCounter = -1;
        this.startedElectionCoordinators = new HashSet<>();
        this.scheduledElectionCoordinators = new HashSet<>();
        this.completedElectionCoordinators = new HashSet<>();
        this.electionTransactionIds = new HashMap<>();
    }

    public static Props props(int id, int minLatency, int maxLatency, int coordinatorBeatInterval) {
        return Props.create(
                Replica.class,
                () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval, Optional.empty()));
    }

    // Props method for automated tests
    public static Props propsWithListener(
            int id, int minLatency, int maxLatency, int coordinatorBeatInterval, ActorRef listener) {
        return Props.create(
                Replica.class,
                () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval, Optional.ofNullable(listener)));
    }

    public int getPosition(int index) {
        if (index < 0 || index >= AbstractReplica.POSITIONS_LIST_LENGTH) {
            throw new IllegalArgumentException(
                    "Index must be between 0 and " + (AbstractReplica.POSITIONS_LIST_LENGTH - 1));
        }
        return positions[index];
    }

    public void setPosition(int index, int value) {
        if (index < 0 || index >= AbstractReplica.POSITIONS_LIST_LENGTH) {
            throw new IllegalArgumentException(
                    "Index must be between 0 and " + (AbstractReplica.POSITIONS_LIST_LENGTH - 1));
        }
        positions[index] = value;
    }

    public ActorRef getCoordinator() {
        return groupOfReplicas.get(coordinatorID);
    }

    ///////////// Sending helpers ////////////
    // public abstract class Msg implements Serializable {};

    /**
     * Broadcasts a message to all replicas in the group.
     * @param msg The message to be broadcasted.
     * @param includeSelf A boolean flag indicating whether to include the sender replica in the broadcast. If true, the message will also be sent to the sender replica; if false, it will be excluded.
     *
     * @apiNote This method will be empowered in the future and will be able to send messages using total ordering
     */
    public void broadcast(Msg msg, boolean includeSelf) {
        if (this.replicaStatus == CrashStatus.CRASHED) {
            return;
        }

        ActorRef target;
        for (Map.Entry<Integer, ActorRef> entry : groupOfReplicas.entrySet()) {
            target = entry.getValue();
            if (!target.equals(getSelf()) || includeSelf) {
                this.tell(msg, target);
            }
        }
        updateCrashStatusCallback(msg);
    }

    /**
     * Broadcasts a message to all replicas in the group except itself.
     *
     * @param msg The message to be broadcasted.
     */
    public void broadcast(Msg msg) {
        broadcast(msg, false);
    }

    /**
     * Sends a message to a specific replica.
     * @param msg The message to be sent.
     * @param target The replica to which the message will be sent.
     * @apiNote This method will be empowered in the future and will be able to send messages using total ordering
     */
    @Override
    public void unicast(Msg msg, ActorRef target) {
        if (this.replicaStatus == CrashStatus.CRASHED) {
            return;
        }

        if (target.equals(getSelf())) {
            return;
        }
        this.tell(msg, target);
        updateCrashStatusCallback(msg);
    }

    /**
     * This method returns true if the replica is currently the coordinator.
     */
    public boolean isCoordinator() {
        return this.id == this.coordinatorID;
    }

    /**
     * Returns the upper bound used while waiting for the next update phase.
     *
     * @return update phase timeout in milliseconds
     */
    long getUpdatePhaseTimeoutDelay() {
        return 4L * getMaxLatencyPlusTolerance();
    }
    ////////////////////////////////////////////

    @Override
    public void initSystem(InitSystem sysInit) {
        this.groupOfReplicas = sysInit.group;
        this.coordinatorID = sysInit.coordinator_id;
        log("Initialized with group of replicas: " + getSystemNumberOfActors() + " replicas, coordinator ID: "
                + coordinatorID);

        // All replicas will use the coordinator-created identity for this heartbeat term.
        ActorRef coordinator = groupOfReplicas.get(coordinatorID);
        if (coordinator == null) {
            throw new IllegalStateException("Cannot initialize heartbeat: coordinator is not in the replica group.");
        }

        TransactionId heartbeatTransactionId;

        if (this.isCoordinator()) {
            heartbeatTransactionId = this.getNextTransactionId();
        } else {
            heartbeatTransactionId = new TransactionId(coordinator, INITIAL_HEARTBEAT_TRANSACTION_SEQUENCE);
        }

        this.heartbeatTransaction = new HeartbeatTransaction(heartbeatTransactionId, this, getEpochPair());

        scheduleTransaction(this.heartbeatTransaction);
    }

    @Override
    public int getSystemNumberOfActors() {
        return this.groupOfReplicas.size();
    }

    @Override
    public void crash(AbstractReplica.Crash how_to_crash) {
        if (how_to_crash.type == AbstractReplica.Crash.Type.Now) {
            this.replicaStatus = CrashStatus.CRASHED;
            log("Replica crashed immediately due to " + how_to_crash.type + " crash.");
            return;
        }
        this.pendingCrash = how_to_crash;
        this.crashCount = 0;
        this.replicaStatus = CrashStatus.PENDING;
    }

    @Override
    public void scheduleTransaction(Transaction transaction) {
        debug("Scheduled transaction: " + transaction.getId());
        this.activeTransactions.add(transaction);
        transaction.start();
    }

    @Override
    public void onTransactionComplete(Transaction transaction) {
        activeTransactions.remove(transaction);
        debug("Transaction completed: " + transaction.getId());
    }

    @Override
    public TransactionId getNextTransactionId() {
        TransactionId id = new TransactionId(this.getSelf(), transactionCounter);
        ++transactionCounter;
        return id;
    }

    /**
     * Returns an ID from the election-specific transaction namespace.
     *
     * Election messages circulate between replicas and are routed using their
     * transaction ID. Keeping their sequence numbers negative prevents them
     * from colliding with heartbeat IDs, which use sequence 0, or with normal
     * replica transaction IDs.
     */
    private TransactionId getNextElectionTransactionId() {
        TransactionId id = new TransactionId(this.getSelf(), electionTransactionCounter);
        --electionTransactionCounter;
        return id;
    }

    /**
     * Returns the coordinator ID of the replica.
     * @return The coordinator ID of the replica.
     */
    public int getCoordinatorID() {
        return this.coordinatorID;
    }

    /**
     * Sets the coordinator ID of the replica.
     * @param coordinatorID The new coordinator ID to be set.
     */
    public void setCoordinatorID(int coordinatorID) {
        this.coordinatorID = coordinatorID;
    }

    /**
     * Returns the current epoch pair of the replica.
     * @return The current epoch pair of the replica.
     */
    public EpochPair getEpochPair() {
        return this.epochPair;
    }

    /**
     * Sets the current epoch pair of the replica.
     * @param epochPair The new epoch pair to be set.
     * @throws IllegalArgumentException if the provided epochPair is null or if it is less than the current epochPair.
     */
    public void setEpochPair(EpochPair epochPair) throws IllegalArgumentException {
        if (epochPair == null) {
            throw new IllegalArgumentException("EpochPair cannot be null");
        }
        if (this.epochPair == null) {
            this.epochPair = epochPair;
            return;
        }
        if (this.epochPair.compareTo(epochPair) > 0) {
            throw new IllegalArgumentException("New epochPair must be greater than or equal to the current epochPair");
        }
        this.epochPair = epochPair;

        if (this.updateSequenceEpoch != epochPair.getEpoch()) {
            this.updateSequenceEpoch = epochPair.getEpoch();
            this.nextUpdateSequence = epochPair.getSequence() + 1;
        } else {
            this.nextUpdateSequence = Math.max(this.nextUpdateSequence, epochPair.getSequence() + 1);
        }
    }

    /**
     * Reserves the next total-order identity for an update started by this
     * coordinator. Reservation is separate from {@link #epochPair}: an update
     * may be in flight before it is committed locally.
     *
     * @return a unique pair for the current coordinator epoch
     * @throws IllegalStateException if this replica is not the coordinator
     */
    EpochPair reserveNextUpdateEpochPair() {
        if (!isCoordinator()) {
            throw new IllegalStateException("Only the coordinator can order updates");
        }

        int currentEpoch = this.epochPair.getEpoch();
        if (this.updateSequenceEpoch != currentEpoch) {
            this.updateSequenceEpoch = currentEpoch;
            this.nextUpdateSequence = this.epochPair.getSequence() + 1;
        }

        EpochPair reserved = new EpochPair(this.updateSequenceEpoch, this.nextUpdateSequence);
        this.nextUpdateSequence++;
        return reserved;
    }

    /**
     * Returns the update history of the replica, which is a map of epoch pairs to their corresponding UpdateTransaction.
     * @return A map containing the update history of the replica.
     */
    public Map<EpochPair, UpdateTransaction> getUpdateHistory() {
        return new HashMap<>(this.updateHistory);
    }

    /**
     * Adds an UpdateTransaction to the update history of the replica.
     * @param updateTransaction The UpdateTransaction to be added to the history.
     */
    public void addUpdateToHistory(EpochPair updateId, UpdateTransaction updateTransaction) {
        if (updateId == null) {
            throw new IllegalArgumentException("Committed updates must have an EpochPair");
        }
        this.updateHistory.put(updateId, updateTransaction);
    }

    @Override
    public final Receive createReceive() {
        return createBaseReceiveBuilder()
                .match(ReadMsg.class, this::onReadMsg)
                .match(UpdateMsg.class, this::onUpdateMsg)
                .match(WriteMsg.class, this::onWriteMsg)
                .match(ElectionMsg.class, this::onElectionMsg)
                .match(ElectionStartMsg.class, this::onElectionStartMsg)
                .match(SynchronizationMsg.class, this::onSynchronizationMsg)
                // Handle TestMsg messages, leave it as last
                .match(ProbeMsg.class, this::onProbeMsg)
                .matchAny(msg -> defaultDispatcher(msg))
                .build();
    }

    /**
     * Delivers the message to the appropriate active Transaction.
     * @param msg Incoming message to be processed by the appropriate Transaction.
     */
    public void onMessage(Msg msg) {
        boolean delivered = false;
        if (this.replicaStatus == CrashStatus.CRASHED) {
            return;
        }
        for (Transaction transaction : new ArrayList<>(activeTransactions)) {
            if (transaction.getId().equals(msg.transactionId)) {
                transaction.computeState(msg);
                delivered = true;
                break;
            }
        }
        if (delivered) {
            updateCrashStatusCallback(msg);
        }
    }

    public void onWriteMsg(WriteMsg msg) {
        WriteTransaction transaction =
                new WriteTransaction(msg.transactionId, this, getEpochPair(), msg.index, msg.value, msg.sender);
        scheduleTransaction(transaction);
    }

    void onUpdateMsg(UpdateMsg msg) {
        // debug("Received UpdateMsg for transaction: " + msg.transactionId);
        // debug("Received UpdateMsg [index: " + msg.index + ", value: " + msg.value + "]");
        if (!msg.transactionId.initiator.equals(this.getSelf())) {
            UpdateTransaction transaction =
                    new UpdateTransaction(msg.transactionId, this, msg.epochPair, msg.index, msg.value, msg.sender);
            this.scheduleTransaction(transaction);
        } else {
            onMessage(msg);
        }
    }

    public void onReadMsg(ReadMsg msg) {
        unicast(
                new ReadResultMsg(msg.transactionId, msg.epochPair, getSelf(), getPosition(msg.index), this.id),
                msg.sender);
    }

    void startElection(int failedCoordinatorId) {
        if (completedElectionCoordinators.contains(failedCoordinatorId)
                || startedElectionCoordinators.contains(failedCoordinatorId)
                || !scheduledElectionCoordinators.add(failedCoordinatorId)) {
            return;
        }

        scheduleToItself(
                electionStartDelay(failedCoordinatorId),
                new ElectionStartMsg(
                        null,
                        getEpochPair(),
                        getSelf(),
                        failedCoordinatorId));
    }

    private long electionStartDelay(int failedCoordinatorId) {
        List<Integer> ringIds = new ArrayList<>(groupOfReplicas.keySet());
        ringIds.sort(Integer::compareTo);

        int failedCoordinatorIndex = ringIds.indexOf(failedCoordinatorId);
        int replicaIndex = ringIds.indexOf(getId());
        if (failedCoordinatorIndex < 0 || replicaIndex < 0) {
            throw new IllegalArgumentException(
                    "Election participants must belong to the replica ring");
        }

        int ringDistance =
                (replicaIndex - failedCoordinatorIndex + ringIds.size())
                        % ringIds.size();
        if (ringDistance == 0) {
            throw new IllegalArgumentException(
                    "The failed coordinator cannot start its own election");
        }

        return (long) ringDistance * getMaxLatencyPlusTolerance();
    }

    private void beginElection(int failedCoordinatorId) {
        if (completedElectionCoordinators.contains(failedCoordinatorId)
                || !startedElectionCoordinators.add(failedCoordinatorId)) {
            return;
        }

        callbackOnElectionStarted(failedCoordinatorId);

        ElectionTransaction transaction = new ElectionTransaction(
                getNextElectionTransactionId(),
                this,
                getEpochPair(),
                failedCoordinatorId,
                groupOfReplicas,
                true);
        electionTransactionIds.put(failedCoordinatorId, transaction.getId());
        scheduleTransaction(transaction);
    }

    public void onElectionStartMsg(ElectionStartMsg message) {
        scheduledElectionCoordinators.remove(message.failedCoordinatorId);
        beginElection(message.failedCoordinatorId);
    }

    public void onElectionMsg(ElectionMsg message) {
        if (this.replicaStatus == CrashStatus.CRASHED) {
            return;
        }

        if (!isValidElectionMessage(message)) {
            return;
        }

        if (completedElectionCoordinators.contains(message.failedCoordinatorId)) {
            return;
        }

        scheduledElectionCoordinators.remove(message.failedCoordinatorId);

        TransactionId currentElectionId =
                electionTransactionIds.get(message.failedCoordinatorId);
        if (currentElectionId != null
                && currentElectionId.equals(message.transactionId)) {
            onMessage(message);
            return;
        }

        if (currentElectionId != null) {
            if (isIncomingElectionPreferred(
                    currentElectionId,
                    message.transactionId)) {
                replaceElection(currentElectionId, message);
                onMessage(message);
                return;
            } else {
                rejectElection(message);
                return;
            }
        } else if (startedElectionCoordinators.contains(message.failedCoordinatorId)) {
            rejectElection(message);
            return;
        }

        ElectionTransaction transaction = new ElectionTransaction(
                message.transactionId,
                this,
                getEpochPair(),
                message.failedCoordinatorId,
                groupOfReplicas,
                false);
        electionTransactionIds.put(
                message.failedCoordinatorId,
                message.transactionId);
        startedElectionCoordinators.add(message.failedCoordinatorId);
        callbackOnElectionStarted(message.failedCoordinatorId);
        scheduleTransaction(transaction);
        onMessage(message);
    }

    private boolean isValidElectionMessage(ElectionMsg message) {
        if (message == null
                || message.transactionId == null
                || message.transactionId.initiator == null
                || message.sender == null
                || !groupOfReplicas.containsValue(message.sender)
                || !groupOfReplicas.containsValue(message.transactionId.initiator)
                || !groupOfReplicas.containsKey(message.failedCoordinatorId)
                || message.failedCoordinatorId == getId()) {
            return false;
        }

        Set<Integer> candidateIds = new HashSet<>();
        for (ElectionTransaction.ElectionCandidate candidate : message.candidates) {
            if (!groupOfReplicas.containsKey(candidate.getReplicaId())
                    || candidate.getReplicaId() == message.failedCoordinatorId
                    || !candidateIds.add(candidate.getReplicaId())) {
                return false;
            }
        }

        return true;
    }

    private boolean isIncomingElectionPreferred(
            TransactionId currentElectionId,
            TransactionId incomingElectionId) {
        int currentInitiatorId = replicaIdFor(currentElectionId.initiator);
        int incomingInitiatorId = replicaIdFor(incomingElectionId.initiator);

        if (incomingInitiatorId != currentInitiatorId) {
            return incomingInitiatorId < currentInitiatorId;
        }
        return incomingElectionId.sequenceNumber < currentElectionId.sequenceNumber;
    }

    private int replicaIdFor(ActorRef actorRef) {
        for (Map.Entry<Integer, ActorRef> entry : groupOfReplicas.entrySet()) {
            if (entry.getValue().equals(actorRef)) {
                return entry.getKey();
            }
        }
        throw new IllegalArgumentException(
                "Election transaction initiator is not in the replica group");
    }

    private void replaceElection(
            TransactionId currentElectionId,
            ElectionMsg incomingMessage) {
        activeTransactions.removeIf(transaction ->
                transaction.getId().equals(currentElectionId));
        electionTransactionIds.remove(incomingMessage.failedCoordinatorId);

        ElectionTransaction transaction = new ElectionTransaction(
                incomingMessage.transactionId,
                this,
                getEpochPair(),
                incomingMessage.failedCoordinatorId,
                groupOfReplicas,
                false);
        electionTransactionIds.put(
                incomingMessage.failedCoordinatorId,
                incomingMessage.transactionId);
        scheduleTransaction(transaction);

        TransactionId currentInitiator = currentElectionId;
        if (!currentInitiator.initiator.equals(getSelf())) {
            unicast(
                    new ElectionTransaction.ElectionRejectMsg(
                            currentElectionId,
                            incomingMessage.epochPair,
                            getSelf()),
                    currentInitiator.initiator);
        }
    }

    private void rejectElection(ElectionMsg message) {
        if (message.transactionId.initiator.equals(getSelf())) {
            return;
        }

        unicast(
                new ElectionTransaction.ElectionRejectMsg(
                        message.transactionId,
                        message.epochPair,
                        getSelf()),
                message.transactionId.initiator);
    }

    void onElectionTransactionCancelled(
            int failedCoordinatorId,
            TransactionId electionTransactionId) {
        if (electionTransactionId.equals(
                electionTransactionIds.get(failedCoordinatorId))) {
            electionTransactionIds.remove(failedCoordinatorId);
        }
    }

    public void onSynchronizationMsg(SynchronizationMsg message) {
        if (this.replicaStatus == CrashStatus.CRASHED) {
            return;
        }

        if (!isValidSynchronization(message)) {
            return;
        }

        applySynchronization(message);
    }

    private boolean isValidSynchronization(SynchronizationMsg message) {
        if (message == null
                || message.transactionId == null
                || message.epochPair == null
                || message.sender == null
                || message.failedCoordinatorId == message.newCoordinatorId
                || coordinatorID != message.failedCoordinatorId) {
            return false;
        }

        ActorRef announcedCoordinator = groupOfReplicas.get(message.newCoordinatorId);
        if (announcedCoordinator == null
                || !announcedCoordinator.equals(message.sender)
                || !message.epochPair.equals(message.newEpochPair)) {
            return false;
        }

        EpochPair currentEpochPair = getEpochPair();
        if (currentEpochPair != null
                && message.newEpochPair.compareTo(currentEpochPair) <= 0) {
            return false;
        }

        return message.getPositions().length == positions.length;
    }

    void completeElectionAsWinner(
            int failedCoordinatorId,
            TransactionId electionTransactionId,
            List<ElectionTransaction.ElectionCandidate> candidates) {
        if (!completedElectionCoordinators.add(failedCoordinatorId)) {
            return;
        }

        electionTransactionIds.remove(failedCoordinatorId);
        ElectionTransaction electionTransaction = findElectionTransaction(
                failedCoordinatorId,
                electionTransactionId);
        if (electionTransaction != null) {
            electionTransaction.enterSynchronizing();
        }

        int maximumEpoch = 0;
        for (ElectionTransaction.ElectionCandidate candidate : candidates) {
            if (candidate.hasObservedUpdate()) {
                maximumEpoch = Math.max(
                        maximumEpoch,
                        candidate.getLatestObservedEpochPair().getEpoch());
            }
        }

        EpochPair newEpochPair = new EpochPair(maximumEpoch + 1, 0);
        coordinatorID = getId();
        setEpochPair(newEpochPair);
        callbackOnCoordinatorElected(getId());

        SynchronizationMsg synchronization = new SynchronizationMsg(
                electionTransactionId,
                newEpochPair,
                getSelf(),
                failedCoordinatorId,
                getId(),
                newEpochPair,
                positions);

        for (Map.Entry<Integer, ActorRef> entry : groupOfReplicas.entrySet()) {
            if (entry.getKey() != getId()) {
                unicast(synchronization, entry.getValue());
            }
        }

        restartHeartbeat(newEpochPair);
        removeElectionTransactions(failedCoordinatorId);
    }

    private void applySynchronization(SynchronizationMsg message) {
        if (!completedElectionCoordinators.add(message.failedCoordinatorId)) {
            return;
        }

        electionTransactionIds.remove(message.failedCoordinatorId);
        for (Transaction transaction : activeTransactions) {
            if (transaction instanceof ElectionTransaction
                    && ((ElectionTransaction) transaction).getFailedCoordinatorId()
                            == message.failedCoordinatorId) {
                ((ElectionTransaction) transaction).enterSynchronizing();
            }
        }

        int[] synchronizedPositions = message.getPositions();
        if (synchronizedPositions.length != positions.length) {
            throw new IllegalArgumentException(
                    "Synchronization positions have an invalid length");
        }

        System.arraycopy(
                synchronizedPositions,
                0,
                positions,
                0,
                positions.length);
        coordinatorID = message.newCoordinatorId;
        setEpochPair(message.newEpochPair);
        callbackOnCoordinatorElected(message.newCoordinatorId);
        restartHeartbeat(message.newEpochPair);
        removeElectionTransactions(message.failedCoordinatorId);
    }

    private void restartHeartbeat(EpochPair epochPair) {
        if (heartbeatTransaction != null) {
            activeTransactions.remove(heartbeatTransaction);
        }

        ActorRef coordinator = groupOfReplicas.get(coordinatorID);
        if (coordinator == null) {
            return;
        }

        heartbeatTransaction = new HeartbeatTransaction(
                new TransactionId(coordinator, INITIAL_HEARTBEAT_TRANSACTION_SEQUENCE),
                this,
                epochPair);
        scheduleTransaction(heartbeatTransaction);
    }

    private void removeElectionTransactions(int failedCoordinatorId) {
        for (Transaction transaction : activeTransactions) {
            if (transaction instanceof ElectionTransaction
                    && ((ElectionTransaction) transaction).getFailedCoordinatorId()
                            == failedCoordinatorId) {
                ((ElectionTransaction) transaction).complete();
            }
        }
        activeTransactions.removeIf(transaction ->
                transaction instanceof ElectionTransaction
                        && ((ElectionTransaction) transaction)
                                .getFailedCoordinatorId() == failedCoordinatorId);
    }

    private ElectionTransaction findElectionTransaction(
            int failedCoordinatorId,
            TransactionId electionTransactionId) {
        for (Transaction transaction : activeTransactions) {
            if (transaction instanceof ElectionTransaction
                    && transaction.getId().equals(electionTransactionId)
                    && ((ElectionTransaction) transaction).getFailedCoordinatorId()
                            == failedCoordinatorId) {
                return (ElectionTransaction) transaction;
            }
        }
        return null;
    }

    /// For testing
    public void onProbeMsg(ProbeMsg msg) {
        if (ProbeTransaction.MSG_START.equals(msg.content)) {
            ProbeTransaction transaction =
                    new ProbeTransaction(msg.transactionId, this, msg.epochPair, msg, msg.sender);
            scheduleTransaction(transaction);
        } else {
            onMessage(msg);
        }
    }

    void defaultDispatcher(Object msg) {
        if (msg instanceof Msg) {
            onMessage((Msg) msg);
        }
    }

    /**
     * This callback handles the counting of messages received by the replica when a pending crash is set.
     * Call this method passing the handled message to update accordingly to its type and the pending crash type.
     * @param msg The message handled by the replica.
     */
    void updateCrashStatusCallback(Msg msg) {
        if (this.replicaStatus == CrashStatus.PENDING) {
                    boolean shouldIncrementCrashCount =
                    switch (msg) {
                        case HeartbeatMsg _, WatchdogExpiredMsg _ -> this.pendingCrash.type == Crash.Type.Heartbeat;
                        case WriteFinishMsg _ -> this.pendingCrash.type == Crash.Type.WriteOK;
                        case UpdateMsg _, UpdateTimeoutMsg _, UpdateAckMsg _, WriteOkMsg _, WriteOkTimeoutMsg _ -> {
                            yield this.pendingCrash.type == Crash.Type.Update;
                        }
                        case ElectionMsg _, ElectionTransaction.ElectionAckMsg _,
                                ElectionTransaction.ElectionAckTimeoutMsg _,
                                ElectionTransaction.ElectionRejectMsg _, SynchronizationMsg _ ->
                                this.pendingCrash.type == Crash.Type.Election;
                        default -> false;
                    };

            if (shouldIncrementCrashCount) {
                this.crashCount++;
                if (this.crashCount >= this.pendingCrash.after_n_messages_of_type) {
                    this.replicaStatus = CrashStatus.CRASHED;
                    log("Replica crashed due to " + this.pendingCrash.type + " crash.");
                }
            }
        }
    }
}
