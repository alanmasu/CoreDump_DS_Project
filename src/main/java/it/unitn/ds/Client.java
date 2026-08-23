package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.ds.ReadTransaction.ReadResultMsg;
import it.unitn.ds.ReadTransaction.ReadTimeoutMsg;
import it.unitn.ds.ProbeTransaction.ProbeMsg;
import it.unitn.ds.Transaction.TransactionId;
import java.util.LinkedList;
import java.util.Optional;
import java.util.Queue;

public class Client extends AbstractClient implements DistributedActor {

    private int transactionCounter;

    private Queue<Transaction> scheduledTransactions;
    private Transaction currentTransaction;

    Client(
            long readTimeoutDelay,
            long writeTimeoutDelay,
            Optional<ActorRef> defaultTargetReplica,
            Optional<ActorRef> listener) {
        super(readTimeoutDelay, writeTimeoutDelay, listener, defaultTargetReplica);
        scheduledTransactions = new LinkedList<>();
        transactionCounter = 0;
    }

    public static Props props(long readTimeoutDelay, long writeTimeoutDelay, Optional<ActorRef> defaultTargetReplica) {
        return Props.create(
                Client.class,
                () -> new Client(readTimeoutDelay, writeTimeoutDelay, defaultTargetReplica, Optional.empty()));
    }

    // Props method for automated tests
    public static Props propsWithListener(
            long readTimeoutDelay, long writeTimeoutDelay, Optional<ActorRef> defaultTargetReplica, ActorRef listener) {
        return Props.create(
                Client.class,
                () -> new Client(
                        readTimeoutDelay, writeTimeoutDelay, defaultTargetReplica, Optional.ofNullable(listener)));
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
        // TODO: Add the correct startEpochPair to the ReadTransaction constructor
        ReadTransaction transaction = new ReadTransaction(getNextTransactionId(), this, null, index, replica);
        scheduleTransaction(transaction);
    }

    @Override
    public void sendWrite(ActorRef replica, int index, int value) {
        WriteTransaction transaction = new WriteTransaction(this.getNextTransactionId(), this, null, index, value, replica);
        scheduleTransaction(transaction);
    }

    @Override
    public TransactionId getNextTransactionId() {
        TransactionId id = new TransactionId(this.getSelf(), transactionCounter);
        ++transactionCounter;
        return id;
    }

    @Override
    public final Receive createReceive() {
        return createBaseReceiveBuilder()
                .match(ProbeMsg.class, this::onProbeMsg)
                .matchAny(msg -> defaultDispatcher(msg))
                .build();
    }

    @Override
    public void scheduleTransaction(Transaction transaction) {
        debug("Scheduled transaction: " + transaction.getId());
        if (this.currentTransaction == null) {
            this.currentTransaction = transaction;
            transaction.start();
        } else {
            this.scheduledTransactions.add(transaction);
        }
    }

    @Override
    public void onTransactionComplete(Transaction transaction) {
        debug("Transaction completed: " + transaction.getId());
        this.currentTransaction = scheduledTransactions.poll();
        if (this.currentTransaction != null) {
            this.currentTransaction.start();
        }
    }

    /**
     * Handles incoming messages.
     * @param msg The message to be handled.
     */
    public void onMessage(Msg msg) {
        if (currentTransaction != null && currentTransaction.getId().equals(msg.transactionId)) {
            currentTransaction.computeState(msg);
        } else {
            debug("Discarded message for inactive transaction: " + msg.transactionId);
        }
    }

    void defaultDispatcher(Object msg) {
        if (msg instanceof Msg) {
            onMessage((Msg) msg);
        }
    }

    /// For testing
    public void onProbeMsg(ProbeMsg msg) {
        if (ProbeTransaction.MSG_START.equals(msg.content)) {
            // TODO: Pass the correct startEpochPair to the ProbeTransaction constructor
            ProbeTransaction transaction = new ProbeTransaction(msg.transactionId, this, null, msg, msg.sender);
            scheduleTransaction(transaction);
        } else {
            onMessage(msg);
        }
    }
}
