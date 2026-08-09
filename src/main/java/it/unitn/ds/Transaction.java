package it.unitn.ds;

import akka.actor.Cancellable;
import akka.actor.AbstractActor;

import java.util.ArrayList;
import java.util.List;

public abstract class Transaction implements Comparable<Transaction> {

    public Cancellable timeout;
    public AbstractActor owner;
    public EpochPair epochPair;
    public int id;

    public List<Msg> history;

    public Transaction(int id, AbstractActor owner) {
        this.owner = owner;
        this.id = id;
        this.timeout = null;
        this.epochPair = null;
        this.history = new ArrayList<>();
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
