package it.unitn.ds;

import akka.actor.Cancellable;
import akka.actor.ActorRef;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public abstract class Transaction implements Comparable<Transaction> {

    public Cancellable timeout;
    public DistributedActor owner;
    public EpochPair epochPair;
    public TransactionId id;
    public List<Msg> history;

    public Transaction(int id, DistributedActor owner) {
        this.owner = owner;
        this.id = new TransactionId(owner.getSelf(), id);
        this.timeout = null;
        this.epochPair = null;
        this.history = new ArrayList<>();
    }

    /**
     * Represents a unique identifier for a transaction, consisting of the owner ActorRef and a progressively increasing transaction ID on the owner.
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
    };


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
        return "Transaction[" +
                "id: " + id +
                ", epochPair: " + epochPair +
                ", owner: " + owner.getSelf().path().name() +
                ", state: " + getState() +
                ']';
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
    
}
