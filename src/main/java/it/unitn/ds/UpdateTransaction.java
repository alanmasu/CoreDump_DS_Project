package it.unitn.ds;

import akka.actor.ActorRef;
import it.unitn.ds.WriteTransaction.WriteFinishMsg;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import scala.concurrent.duration.Duration;

public class UpdateTransaction extends Transaction {

    /** Index of the element to update */
    protected final int index;

    /** Value to update the element with */
    protected final int value;

    /** Destination ActorRef used for unicast communication, coord do not use it*/
    protected final Optional<ActorRef> destination;

    /** Connected WriteTransactionId used to identify the WriteTransaction to complete after this one*/
    protected Optional<TransactionId> writeTid = Optional.empty();

    private int coordinatorAckCount; // Count of the number of acknowledgments received from the coordinator

    protected UpdateTransactionState state;

    // /** Constructor for UpdateTransaction whit no optional parameters -> usefull for coordinator*/
    // public UpdateTransaction(TransactionId id, Replica owner, EpochPair startEpochPair, int index, int value) {
    //     this(id, owner, startEpochPair, index, value, null, null);
    // }

    /** Constructor for UpdateTransaction with destination ActorRef -> usefull for update participant*/
    public UpdateTransaction(
            TransactionId id, Replica owner, EpochPair startEpochPair, int index, int value, ActorRef destination) {
        this(id, owner, startEpochPair, index, value, destination, null);
    }

    /** Constructor for UpdateTransaction with destination ActorRef and connected WriteTransactionId -> usefull for update trigger replica
     *
     * @param id TransactionId of the UpdateTransaction
     * @param owner Replica that owns the UpdateTransaction
     * @param startEpochPair EpochPair in which the UpdateTransaction is started
     * @param index Index of the element to update
     * @param value Value to update the element with
     * @param destination An ActorRef this transaction uses for unicast communication, gernerally the coordinator.
     * @param writeTid Connected WriteTransactionId used to identify the WriteTransaction to complete after this one
     */
    public UpdateTransaction(
            TransactionId id,
            Replica owner,
            EpochPair startEpochPair,
            int index,
            int value,
            ActorRef destination,
            TransactionId writeTid) {
        super(id, owner, startEpochPair);
        this.index = index;
        this.value = value;
        if (destination == null) {
            this.destination = Optional.empty();
        } else {
            this.destination = Optional.of(destination);
        }
        if (writeTid == null) {
            this.writeTid = Optional.empty();
        } else {
            this.writeTid = Optional.of(writeTid);
        }
        this.state = UpdateTransactionState.INIT;
        this.coordinatorAckCount = 0;
    }

    ////////////// Class subtypes //////////////
    /// Here will be defined all the subtypes used in the WriteTransaction class
    ////////////////////////////////////////////
    public static enum UpdateTransactionState {
        INIT,
        WAITING_UPDATE,
        WAITING_ACK,
        WAITING_WRITEOK,
        COMMITTED,
        WAITING_ELECTION;

        @Override
        public String toString() {
            return this.name();
        }
    }

    public static class UpdateMsg extends Msg {
        public final int index;
        public final int value;

        public UpdateMsg(TransactionId transactionId, EpochPair epochPair, ActorRef sender, int index, int value) {
            super(transactionId, epochPair, sender);
            this.index = index;
            this.value = value;
        }
    }

    public static class UpdateTimeoutMsg extends Msg {
        public UpdateTimeoutMsg(TransactionId transactionId, EpochPair epochPair, ActorRef sender) {
            super(transactionId, epochPair, sender);
        }
    }

    public static class UpdateAckMsg extends Msg {
        public UpdateAckMsg(TransactionId transactionId, EpochPair epochPair, ActorRef sender) {
            super(transactionId, epochPair, sender);
        }
    }

    public static class WriteOkMsg extends Msg {
        public final int index;
        public final int value;

        public WriteOkMsg(TransactionId transactionId, EpochPair epochPair, ActorRef sender, int index, int value) {
            super(transactionId, epochPair, sender);
            this.index = index;
            this.value = value;
        }
    }

    public static class WriteOkTimeoutMsg extends Msg {
        public WriteOkTimeoutMsg(TransactionId transactionId, EpochPair epochPair, ActorRef sender) {
            super(transactionId, epochPair, sender);
        }
    }

    ///////////////////////////////////////////

    ///////////// Overiden Methods /////////////
    /// Here will be defined all the subtypes used in the WriteTransaction class
    ////////////////////////////////////////////
    @Override
    public String getState() {
        return state.toString();
    }

    @Override
    public void computeState(Msg msg) {
        Replica replica = (Replica) this.owner;

        if (replica.isCoordinator()) {
            coordinatorStateMachine(replica, msg);
        } else {
            replicaStateMachine(replica, msg);
        }
    }

    @Override
    public void start() throws IllegalStateException {
        Replica replica = (Replica) this.owner;
        if (this.state != UpdateTransactionState.INIT) {
            throw new IllegalStateException(
                    "UpdateTransaction " + this.getId() + " is not in INIT state, cannot start.");
        }
        // replica.debug("Started UpdateTransaction: " + this.getId());
        if (replica.isCoordinator()) {
            // Case 1: Coordinator replica who received an UpdateMsg from a participant
            startAsCoordinator(replica);
        } else {
            if (this.writeTid.isPresent()) {
                // Case 2: Participant replica who wants to update an element
                forwardToCoordinator(replica);
            } else if (this.destination.isPresent() && !replica.isCoordinator()) {
                // Case 3: Participant replica who received an UpdateMsg from the coordinator
                startAsReplica(replica);
            } else {
                throw new IllegalStateException(
                        "UpdateTransaction " + this.getId() + " is not able to start, invalid state or parameters.");
            }
        }
    }
    //////////////////////////////////////////

    protected void forwardToCoordinator(Replica replica) {
        replica.debug("Forwarding UpdateTransaction " + this.getId() + " to coordinator | index: " + this.index
                + " value: " + this.value);
        this.state = UpdateTransactionState.WAITING_UPDATE;
        UpdateMsg updateMsg = new UpdateMsg(this.getId(), null, replica.getSelf(), this.index, this.value);
        replica.unicast(updateMsg, this.destination.get());
        this.timeout = replica.scheduleToItself(
                replica.getMaxLatency() * 3,
                new UpdateTimeoutMsg(this.getId(), this.startEpochPair, replica.getSelf()));
    }

    protected void startAsReplica(Replica replica) {
        replica.debug("Started UpdateTransaction " + this.getId() + " as replica | index: " + this.index + " value: "
                + this.value);
        this.state = UpdateTransactionState.WAITING_WRITEOK;
        UpdateAckMsg updateAckMsg = new UpdateAckMsg(this.getId(), this.startEpochPair, replica.getSelf());
        replica.unicast(updateAckMsg, this.destination.get());
        this.timeout = replica.scheduleToItself(
                replica.getMaxLatency() * 3,
                new UpdateTimeoutMsg(this.getId(), this.startEpochPair, replica.getSelf()));
    }

    protected void startAsCoordinator(Replica replica) {
        replica.debug("Started UpdateTransaction " + this.getId() + " as coordinator | index: " + this.index
                + " value: " + this.value);
        UpdateMsg updateMsg =
                new UpdateMsg(this.getId(), this.startEpochPair, replica.getSelf(), this.index, this.value);
        replica.broadcast(updateMsg);
        this.coordinatorAckCount = 1; // Count the coordinator itself as an acknowledgment
        this.state = UpdateTransactionState.WAITING_ACK;
    }

    protected void coordinatorStateMachine(Replica replica, Msg msg) {
        if (msg instanceof UpdateAckMsg) {
            this.coordinatorAckCount++;
            if (this.coordinatorAckCount >= (replica.getSystemNumberOfActors() / 2 + 1)
                    && this.state == UpdateTransactionState.WAITING_ACK) {
                WriteOkMsg writeOk =
                        new WriteOkMsg(this.getId(), this.startEpochPair, replica.getSelf(), this.index, this.value);
                replica.broadcast(writeOk);
                // TODO: implement the EpochPair update
                // TODO: Implement the storage to the history of the replica
                this.state = UpdateTransactionState.COMMITTED;
                replica.callbackOnUpdateApplied(writeOk.index, writeOk.value);
                replica.onTransactionComplete(this);
            }
        }
    }

    protected void replicaStateMachine(Replica replica, Msg msg) {
        if (msg instanceof UpdateMsg) {
            if (this.timeout != null) {
                this.timeout.cancel();
            }
            UpdateAckMsg updateAckMsg = new UpdateAckMsg(this.getId(), this.startEpochPair, replica.getSelf());
            replica.unicast(updateAckMsg, this.destination.get());
            this.state = UpdateTransactionState.WAITING_WRITEOK;
            this.timeout = replica.getContext()
                    .system()
                    .scheduler()
                    .scheduleOnce(
                            Duration.create(replica.getMaxLatency() * 3, TimeUnit.MILLISECONDS),
                            replica.getSelf(),
                            new WriteOkTimeoutMsg(this.getId(), this.startEpochPair, replica.getSelf()),
                            replica.getContext().system().dispatcher(),
                            replica.getSelf());
        } else if (msg instanceof WriteOkMsg) {
            WriteOkMsg writeOkMsg = (WriteOkMsg) msg;
            if (this.timeout != null) {
                this.timeout.cancel();
            }
            // TODO: Update the list value on the replica
            // TODO: Update the EpochPair on the replica
            this.state = UpdateTransactionState.COMMITTED;
            owner.onTransactionComplete(this);
            replica.callbackOnUpdateApplied(writeOkMsg.index, writeOkMsg.value);
            if (this.writeTid.isPresent()) {
                WriteFinishMsg writeFinishMsg =
                        new WriteFinishMsg(this.writeTid.get(), this.startEpochPair, replica.getSelf());
                replica.getSelf().tell(writeFinishMsg, replica.getSelf());
            }
        }
        // else if (msg instanceof UpdateTimeoutMsg || msg instanceof WriteOkTimeoutMsg){
        //     this.timeout = null;
        //     this.state = UpdateTransactionState.WAITING_ELECTION;
        //     // TODO: Schedule the ElectionTransaction here.
        //     replica.scheduleTransaction(null);
        //     // TODO: Understend what to to here, if terminate this transaction, or wait the election
        // }
    }
}
