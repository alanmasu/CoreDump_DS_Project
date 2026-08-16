package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public abstract class Transaction implements Comparable<Transaction> {

    protected Cancellable timeout;
    protected final DistributedActor owner;
    protected EpochPair epochPair;
    private final TransactionId id;
    protected final List<Msg> history;

    public Transaction(TransactionId id, DistributedActor owner) {
        this.owner = owner;
        this.id = id;
        this.timeout = null;
        this.epochPair = null;
        this.history = new ArrayList<>();
    }

    /**
     * Represents a unique identifier for a transaction, consisting of the initiator ActorRef and a progressively increasing transaction ID on the initiator.
     */
    public static class TransactionId implements Serializable {
        final ActorRef initiator;
        final int transactionId;

        public TransactionId(ActorRef initiator, int transactionId) {
            this.initiator = initiator;
            this.transactionId = transactionId;
        }

        @Override
        public String toString() {
            return "<" + initiator.path().name() + ", " + transactionId + ">";
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            TransactionId other = (TransactionId) obj;
            return this.initiator.equals(other.initiator) && this.transactionId == other.transactionId;
        }

        @Override
        public int hashCode() {
            int result = initiator.hashCode();
            result = 31 * result + Integer.hashCode(transactionId);
            return result;
        }
    }
    ;

    /**
     * Represents the parameters for starting a transaction asynchronously.
     */
    public interface StartParameters {}
    ;

    public TransactionId getId() {
        return id;
    }

    @Override
    public int compareTo(Transaction other) {
        if (this.epochPair == null && other.epochPair == null) {
            return 0;
        } else if (this.epochPair == null) {
            return -1;
        } else if (other.epochPair == null) {
            return 1;
        } else {
            return this.epochPair.compareTo(other.epochPair);
        }
    }

    @Override
    public String toString() {
        return "Transaction[" + "id: "
                + id + ", epochPair: "
                + epochPair + ", owner: "
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
