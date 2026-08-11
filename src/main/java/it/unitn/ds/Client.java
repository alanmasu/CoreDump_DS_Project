package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.ds.Transaction.TransactionId;

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
                .match(ReadResult.class, this::onMessage)
                .match(TestMsg.class, this::onTestMsg)
                .build();
    }

    @Override
    public void onTransactionComplete(Transaction transaction) {
        activeTransactions.remove(transaction);
    }

    @Override
    public void onMessage(Msg msg) {
        debug("Received message: " + msg.toString());
        if (msg instanceof ReadResult && listener.isPresent()) {
            listener.get().tell(msg, self());
        }

        // FOR TESTING
        if(msg instanceof TestMsg && listener.isPresent()){
            listener.get().tell(msg, self());
        }
    }

    /// For testing
    public void onTestMsg(TestMsg msg) {
        onMessage(msg);
        // unicast(new TestMsg(msg.transactionId, msg.epochPair, self(), msg.content), msg.sender);
        if(msg instanceof TestMsg && listener.isPresent()){
            TransactionId tId = new TransactionId(  msg.transactionId.owner, 
                                                    msg.transactionId.transactionId + 1);

            TestMsg responseMsg = new TestMsg(  tId, 
                                                msg.epochPair, 
                                                msg.sender, 
                                                msg.content);

            listener.get().tell(responseMsg, self());
        }
    }
       
}           
