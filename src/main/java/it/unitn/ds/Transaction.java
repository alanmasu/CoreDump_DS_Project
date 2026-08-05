package it.unitn.ds;

import akka.actor.Cancellable;
import akka.actor.ActorRef;


public abstract class Transaction{

    protected Cancellable timeout;
    protected ActorRef owner;


    Transaction(ActorRef owner, Cancellable timeout) {
        this.owner = owner;
        this.timeout = timeout;
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
}
