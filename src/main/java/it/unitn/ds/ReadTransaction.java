package it.unitn.ds;

import java.util.concurrent.TimeUnit;

import akka.actor.ActorRef;
import scala.concurrent.duration.Duration;

public class ReadTransaction extends Transaction {

    private final int index;
    protected final ActorRef destination;
    private ReadTransactionState state;
    protected final Client client;


    public ReadTransaction(TransactionId id, Client owner, EpochPair startEpocPair, int index, ActorRef destination) {
        super(id, owner, startEpocPair);
        this.index = index;
        this.destination = destination;
        this.state = ReadTransactionState.INIT;
        this.client = (Client) this.owner;
    }


    public static enum ReadTransactionState {
        INIT, 
        WAITING_RESULT,
        DONE,
        TIMEOUT
    }

    public static class ReadMsg extends Msg {
        public final int index;

        public ReadMsg(TransactionId transactionId, EpochPair epochPair, ActorRef sender, int index) {
            super(transactionId, epochPair, sender);
            this.index = index;
        }
    }

    public static class ReadTimeoutMsg extends Msg {

        public ReadTimeoutMsg(TransactionId transactionId, EpochPair epochPair, ActorRef sender) {
            super(transactionId, epochPair, sender);
        }
    }

    public static class ReadResultMsg extends Msg {
        public final int replicaId;
        public final int value;

        public ReadResultMsg(TransactionId transactionId, EpochPair epochPair, ActorRef sender, int value, int replicaId) {
            super(transactionId, epochPair, sender);
            this.replicaId = replicaId;
            this.value = value;
        }
    }

    @Override
    public String getState() {
        return state.name();
    }

    @Override
    public void start() {
        if (state != ReadTransactionState.INIT) {
            throw new IllegalStateException("Cannot start a transaction that is not in INIT state.");
        }
        ReadMsg readMsg = new ReadMsg(this.getId(), null, owner.getSelf(), index);
        owner.unicast(readMsg, destination);
        state = ReadTransactionState.WAITING_RESULT;
        this.timeout = client.getContext().system().scheduler().scheduleOnce(
                Duration.create(((Client) owner).getReadTimeoutDelay(), TimeUnit.MILLISECONDS),
                client.getSelf(),
                new ReadTimeoutMsg(this.getId(), null, owner.getSelf()),
                client.getContext().system().dispatcher(),
                client.getSelf()
        );
    }

    @Override
    public void computeState(Msg msg) {
        if (msg instanceof ReadResultMsg) {
            ReadResultMsg readResultMsg = (ReadResultMsg) msg;
            state = ReadTransactionState.DONE;
            if (this.timeout != null){
                this.timeout.cancel();
            }
            client.callbackOnReadResult(new AbstractClient.ReadResult(true, this.index, readResultMsg.value, readResultMsg.replicaId));
            owner.onTransactionComplete(this);
        } else if (msg instanceof ReadTimeoutMsg) {
            state = ReadTransactionState.TIMEOUT;
            client.callbackOnReadTimeout(new AbstractClient.ReadTimeout(owner.getSelf(), this.destination, this.index));
            owner.onTransactionComplete(this);
        } else {
            throw new IllegalArgumentException("Unexpected message type: " + msg.getClass().getName() + " in transaction " + this.getId());
        }
    }
}
