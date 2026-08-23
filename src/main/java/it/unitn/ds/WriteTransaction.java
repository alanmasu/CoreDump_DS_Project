package it.unitn.ds;

import java.util.concurrent.TimeUnit;

import akka.actor.ActorRef;
import it.unitn.ds.AbstractClient.WriteResult;
import it.unitn.ds.AbstractClient.WriteTimeout;
import scala.concurrent.duration.Duration;

public class WriteTransaction extends Transaction {


    protected WriteTransactionState state;
    protected WriteTransactionStartParameters startParameters;
    protected int index;
    protected int value;
    protected ActorRef destination;


    public WriteTransaction(TransactionId id, DistributedActor owner, EpochPair startEpochPair, int index, int value, ActorRef targetActor) {
        super(id, owner, startEpochPair);
        this.state = WriteTransactionState.INIT;
        this.startParameters = new WriteTransactionStartParameters();
        this.index = index;
        this.value = value;
        this.destination = targetActor;
        startParameters.initialMsg = new WriteMsg(id, startEpochPair, owner.getSelf(), index, value);
        startParameters.targetActor = targetActor;
    }

    ////////////// Class subtypes //////////////
    /// Here will be defined all the subtypes used in the WriteTransaction class
    ////////////////////////////////////////////
    public static enum WriteTransactionState {
        INIT,
        WAITING_RESULT,
        WAITING_UPDATE,
        TIMEOUT,
        DONE;

        @Override
        public String toString() {
            return this.name();
        }
    }

    public static class WriteTransactionStartParameters implements StartParameters {
        Msg initialMsg;
        ActorRef targetActor;
    }

    public static abstract class WriteTransactionMsg extends Msg {
        public final int index;
        public final int value;

        public WriteTransactionMsg(TransactionId transactionId, EpochPair epochPair, ActorRef sender, int index, int value) {
            super(transactionId, epochPair, sender);
            this.index = index;
            this.value = value;
        }
    }

    public static class WriteMsg extends WriteTransactionMsg {
        public WriteMsg(TransactionId transactionId, EpochPair epochPair, ActorRef sender, int index, int value) {
            super(transactionId, epochPair, sender, index, value);
        }
    }

    public static class WriteResultMsg extends WriteTransactionMsg {
        public final int replicaId;
        public WriteResultMsg(TransactionId transactionId, EpochPair epochPair, ActorRef sender, int index, int value, int replicaId) {
            super(transactionId, epochPair, sender, index, value);
            this.replicaId = replicaId;
        }
    }

    public static class WriteTimeoutMsg extends Msg {
        public WriteTimeoutMsg(TransactionId transactionId, EpochPair epochPair, ActorRef sender) {
            super(transactionId, epochPair, sender);
        }
    }

    public static class WriteFinishMsg extends Msg {
        public WriteFinishMsg(TransactionId transactionId, EpochPair epochPair, ActorRef sender) {
            super(transactionId, epochPair, sender);
        }
    }
    
    ////////////////////////////////////////////

    ///////////// Overiden Methods /////////////
    /// Here will be defined all the subtypes used in the WriteTransaction class
    ////////////////////////////////////////////
    @Override
    public String getState() {
        return state.toString();
    }

    @Override
    public void computeState(Msg msg) {
        if(this.owner instanceof Client) {
            clientStateMachine(msg);
        } else if(this.owner instanceof Replica) {
            replicaStateMachine(msg);
        }
        
    }

    @Override
    public void start(){
        owner.debug("Started WriteTransaction: " + this.getId());
        if(this.owner instanceof Client) {
            Client client = (Client) this.owner;
            client.unicast(startParameters.initialMsg, startParameters.targetActor);
            state = WriteTransactionState.WAITING_RESULT;
            this.timeout = client.scheduleToItself(
                                    client.getWriteTimeoutDelay(),
                                    new WriteTimeoutMsg(this.getId(), null, client.getSelf())
                                );
        }
        // // TODO: Create a new UpdateTransaction for the replica to handle the update
        // else if(this.owner instanceof Replica) {
        //     // Replica replica = (Replica) this.owner;
        //     // this.state = WriteTransactionState.WAITING_UPDATE;
        //     // Transaction transaction = new UpdateTransaction(replica.getNextTransactionId(), replica, this.startEpochPair, this.index, this.value, this.destination);
        //     // replica.scheduleTransaction(transaction);
        // }

    }    
    ////////////////////////////////////////////
    
    // State machine
    void clientStateMachine(Msg msg) {
        Client client = (Client) this.owner;
        if(msg instanceof WriteResultMsg) {
            WriteResultMsg writeResultMsg = (WriteResultMsg) msg;
            // The following line is not a protocol send, but a way to sand back to the client the result but changing the message type to WriteResult as 
            WriteResult res = new WriteResult(true, writeResultMsg.index, writeResultMsg.value, writeResultMsg.replicaId);
            client.callbackOnWriteResult(res);
            owner.onTransactionComplete(this);
            if(this.timeout != null) {
                this.timeout.cancel();
            }
            this.state = WriteTransactionState.DONE;
        } else if (msg instanceof WriteTimeoutMsg) {
            client.callbackOnWriteTimeout(new WriteTimeout(owner.getSelf(), this.destination, this.index, this.value));
            owner.onTransactionComplete(this);
            this.state = WriteTransactionState.TIMEOUT;
        } else {
            throw new IllegalArgumentException("Received unexpected message type: " + msg.getClass().getName() + " in WriteTransaction with id: " + this.getId());
        }
    }

    void replicaStateMachine(Msg msg) {
        Replica replica = (Replica) this.owner;
        if(msg instanceof WriteFinishMsg) {
            WriteResultMsg writeResultMsg = new WriteResultMsg(this.getId(), this.startEpochPair, owner.getSelf(), this.index, this.value, replica.getId());
            replica.unicast(writeResultMsg, this.destination);
            replica.onTransactionComplete(this);
            this.state = WriteTransactionState.DONE;    
        }
    }
}
