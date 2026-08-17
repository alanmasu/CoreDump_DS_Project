package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.ds.TestTransaction.TestMsg;
import it.unitn.ds.Transaction.TransactionId;

import java.util.LinkedList;
import java.util.Optional;
import java.util.Queue;

public class Client extends AbstractClient implements DistributedActor{

    private int transactionCounter;

    Queue<Transaction> scheduledTransactions;
    Transaction currentTransaction;
    
    Client(long readTimeoutDelay, long writeTimeoutDelay, Optional<ActorRef> defaultTargetReplica, Optional<ActorRef> listener) {
        super(readTimeoutDelay, writeTimeoutDelay, listener, defaultTargetReplica);
        scheduledTransactions = new LinkedList<>();
        transactionCounter = 0;
    }

    public static Props props(long readTimeoutDelay, long writeTimeoutDelay, Optional<ActorRef> defaultTargetReplica) {
        return Props.create(Client.class, () -> new Client(readTimeoutDelay, writeTimeoutDelay, defaultTargetReplica, Optional.empty()));
    }

    // Props method for automated tests
    public static Props propsWithListener(long readTimeoutDelay, long writeTimeoutDelay, Optional<ActorRef> defaultTargetReplica, ActorRef listener) {
        return Props.create(Client.class, () -> new Client(readTimeoutDelay, writeTimeoutDelay, defaultTargetReplica, Optional.ofNullable(listener)));
    }

    /**
     * Sends a message to a specific actor.
     * @param msg The message to be sent.
     * @param target The actor to which the message will be sent.
     */
    @Override
    public void unicast(Msg msg, ActorRef target) {
        target.tell(msg, this.getSelf());
    }

    @Override
    public void sendRead(ActorRef replica, int index) {
        // TODO: implement        
    }

    @Override
    public void sendWrite(ActorRef replica, int index, int value) {
        // TODO: implement
    }

    @Override
    public TransactionId getNextTransactionId() {
        return new TransactionId(this.getSelf(), transactionCounter++);
    }

    @Override
    public final Receive createReceive() {
        return createBaseReceiveBuilder()
                .match(TestMsg.class, this::onTestMsg)
                .build();
    }

    @Override
    public void scheduleTransaction(Transaction transaction) {
        debug("Scheduled transaction: " + transaction.getId());
        if(this.currentTransaction == null) {
            this.currentTransaction = transaction;
            transaction.start();
        }else {
            this.scheduledTransactions.add(transaction);
        }
    }   

    @Override
    public void onTransactionComplete(Transaction transaction) {
        debug("Transaction completed: " + transaction.getId());
        Transaction nextTransaction = scheduledTransactions.poll();
        if(nextTransaction != null) {
            this.currentTransaction = nextTransaction;
            nextTransaction.start();
        } else {
            this.currentTransaction = null;
        }
    }

    public void onMessage(Msg msg) {
        currentTransaction.computeState(msg);
    }

    /// For testing
    public void onTestMsg(TestMsg msg) {
        if(msg.content.equals("start")){
            TestTransaction transaction = new TestTransaction(msg.transactionId,this, msg.epochPair, msg, msg.sender);
            scheduleTransaction(transaction);
        }else {
            onMessage(msg);
        }
    }
       

}           
