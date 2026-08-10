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
     * Called when a message is received and before handling it. The implementing actor can process the received message and perform any necessary actions.
     *
     * @param msg The received message.
     */
    public void onMessage(Msg msg);
}
