package it.unitn.ds;


import akka.actor.ActorRef;

public interface DistributedActor {
    /**
     * Returns the self ActorRef of the implementing actor.
     *  
    */
    public ActorRef getSelf();

    /**
     * Called when a transaction is completed. The implementing actor can perform any necessary actions upon transaction completion.
     *
     * @param transaction The completed transaction.
     */
    public void onTransactionComplete(Transaction transaction);


    /**
     * Schedules a transaction for processing.
     *
     * @param transaction The transaction to be scheduled.
     */
    public void scheduleTransaction(Transaction transaction);

    /**
     * Sends a message to a specific actor.
     *
     * @param msg The message to be sent.
     * @param target The actor to which the message will be sent.
     */
    public void unicast(Msg msg, ActorRef target);


    // TODO: this is not usefull for the Client Class
    // /**
    //  * Sends a message to all actors in the system.
    //  *
    //  * @param msg The message to be sent.
    //  */
    // public void broadcast(Msg msg);

    /**
     * Logs a message for the official log.
     *
     * @param message The message to be logged.
     */
    public void log(String message);

    /**
     * Logs a message for debugging purposes.
     *
     * @param message The message to be logged for debugging.
     */
    public void debug(String message);
}
