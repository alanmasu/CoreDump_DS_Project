package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public abstract class Transaction {

    protected Cancellable timeout;
    protected final DistributedActor owner;
    protected final EpochPair startEpochPair;
    private final TransactionId id;
    protected final List<Msg> history;

    public Transaction(TransactionId id, DistributedActor owner, EpochPair startEpochPair) {
        this.owner = owner;
        this.id = id;
        this.startEpochPair = startEpochPair;
        this.history = new ArrayList<>();
    }

    /**
     * Represents a unique identifier for a transaction, consisting of the initiator ActorRef and a progressively increasing transaction ID on the initiator.
     */
    public static class TransactionId implements Serializable {
        final ActorRef initiator;
        final int sequenceNumber;

        public TransactionId(ActorRef initiator, int sequenceNumber) {
            this.initiator = initiator;
            this.sequenceNumber = sequenceNumber;
        }

        @Override
        public String toString() {
            return "<" + initiator.path().name() + ", " + sequenceNumber + ">";
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            TransactionId other = (TransactionId) obj;
            return this.initiator.equals(other.initiator) && this.sequenceNumber == other.sequenceNumber;
        }

        @Override
        public int hashCode() {
            int result = initiator.hashCode();
            result = 31 * result + Integer.hashCode(sequenceNumber);
            return result;
        }
    }

    /**
     * Represents the parameters for starting a transaction asynchronously.
     */
    public interface StartParameters {}

    public TransactionId getId() {
        return id;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        Transaction other = (Transaction) obj;
        return this.id.equals(other.id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public String toString() {
        return "Transaction[" + "id: "
                + id + ", startEpochPair: "
                + startEpochPair + ", owner: "
                + owner.getSelf().path().name() + ", state: "
                + getState() + ']';
    }

    /**
     * Returns the current state of the transaction.
     * @return the current state of the transaction as a String.
     */
    public abstract String getState();

    /**
     * Computes the new state of the transaction based on the given message.
     * This also calls the method to update the state of ActorRef who owns the transaction.
     *
     * @param msg the message to process and compute the new state.
     */
    public abstract void computeState(Msg msg);

    /**
     * Starts the transaction from scratch, initializing any necessary state and sending the initial messages to the relevant actors. <p>
     * This method is usefull to start a transaction
     */
    public abstract void start();
}
