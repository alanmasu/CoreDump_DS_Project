package it.unitn.ds;

import akka.actor.Cancellable;
import akka.actor.Actor;

import java.util.List;

public abstract class Transaction{

    protected Cancellable timeout;
    protected Actor owner;
    protected EpochPair epochPair;
    public int id;

    List<Msg> history;

    Transaction(Actor owner, Cancellable timeout) {
        this.owner = owner;
        this.timeout = timeout;
        this.epochPair = null;
    }

    /**
     * Returns the current state of the transaction.
     * @return the current state of the transaction as a String.
     */
    abstract String getState();

    /**
     * Computes the new state of the transaction based on the given message. 
     * This also calls the method to update the state of ActorRef who owns the transaction.
     * 
     * @param msg the message to process and compute the new state.
     */
    abstract void computeState(Msg msg);

    /**
     * This method is called when a new coordinator is elected. 
     * It allows the transaction to perform any necessary actions to conclude the transactions previously handled by the old coordinator.
     */
    abstract void onCoordinatorElected();
}
