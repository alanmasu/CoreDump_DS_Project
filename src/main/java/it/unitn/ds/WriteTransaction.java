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


    public WriteTransaction(TransactionId id, DistributedActor owner, EpochPair epochPair, int index, int value, ActorRef targetActor) {
        super(id, owner);
        this.state = WriteTransactionState.INIT;
        this.startParameters = new WriteTransactionStartParameters();
        this.epochPair = epochPair;
        this.index = index;
        this.value = value;
        this.destination = targetActor;
        startParameters.initialMsg = new WriteMsg(id, epochPair, owner.getSelf(), index, value);
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
            client.getContext().system().scheduler().scheduleOnce(
               Duration.create(client.getWriteTimeoutDelay(), TimeUnit.MILLISECONDS), 
               client.getSelf(), 
               new WriteTimeoutMsg(this.getId(), epochPair, client.getSelf()), 
               client.getContext().system().dispatcher(),
               client.getSelf()
            );

        }else if(this.owner instanceof Replica) {
            // // TODO: Create a new UpdateTransaction for the replica to handle the update
            // Replica replica = (Replica) this.owner;
            // this.state = WriteTransactionState.WAITING_UPDATE;
            // ((Replica)this.owner).scheduleTransaction(new UpdateTransaction(owner.getNextTransactionId(), owner, this.getId(), this.index, this.value));
        }
            

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
            this.state = WriteTransactionState.DONE;
        } else if(msg instanceof WriteTimeoutMsg) {
            owner.getSelf().tell(new WriteTimeout(owner.getSelf(), this.destination, this.index, this.value), owner.getSelf());
            owner.onTransactionComplete(this);
            this.state = WriteTransactionState.TIMEOUT;
        } else {
            throw new IllegalArgumentException("Received unexpected message type: " + msg.getClass().getName() + " in WriteTransaction with id: " + this.getId());
        }
    }

    void replicaStateMachine(Msg msg) {
        Replica replica = (Replica) this.owner;
        if(msg instanceof WriteFinishMsg) {
            WriteResultMsg writeResultMsg = new WriteResultMsg(this.getId(), this.epochPair, owner.getSelf(), this.index, this.value, replica.getId());
            replica.unicast(writeResultMsg, this.destination);
            replica.onTransactionComplete(this);
            this.state = WriteTransactionState.DONE;    
        }
    }
}
