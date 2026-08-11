package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.ds.Transaction.TransactionId;
import it.unitn.ds.TestTransaction.TestMsg;

import java.util.LinkedList;
import java.util.Optional;

public class Client extends AbstractClient implements DistributedActor{

    LinkedList<Transaction> activeTransactions;
    
    Client(long readTimeoutDelay, long writeTimeoutDelay, Optional<ActorRef> defaultTargetReplica, Optional<ActorRef> listener) {
        super(readTimeoutDelay, writeTimeoutDelay, listener, defaultTargetReplica);
        activeTransactions = new LinkedList<>();
    }

    public static Props props(long readTimeoutDelay, long writeTimeoutDelay, Optional<ActorRef> defaultTargetReplica) {
        return Props.create(Client.class, () -> new Client(readTimeoutDelay, writeTimeoutDelay, defaultTargetReplica, Optional.empty()));
    }

    // Props method for automated tests
    public static Props propsWithListener(long readTimeoutDelay, long writeTimeoutDelay, Optional<ActorRef> defaultTargetReplica, ActorRef listener) {
        return Props.create(Client.class, () -> new Client(readTimeoutDelay, writeTimeoutDelay, defaultTargetReplica, Optional.ofNullable(listener)));
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
    public final Receive createReceive() {
        return createBaseReceiveBuilder()
                .match(TestMsg.class, this::onTestMsg)
                .build();
    }

    @Override
    public void onTransactionComplete(Transaction transaction) {
        activeTransactions.remove(transaction);
    }

    @Override
    public void onMessage(Msg msg) {
        for(Transaction transaction : activeTransactions){
            if(transaction.id.equals(msg.transactionId)){
                transaction.computeState(msg);
                return;
            }
        }
    }

    /// For testing
    public void onTestMsg(TestMsg msg) {
        if(msg.content.equals("start")){
            TransactionId tId = new TransactionId(this.getSelf(), this.activeTransactions.size() + 1);
            TestTransaction transaction = new TestTransaction(tId.transactionId, this);
            this.activeTransactions.add(transaction);
        }
        onMessage(msg);
    }
       

}           
