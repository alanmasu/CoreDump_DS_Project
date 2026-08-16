package it.unitn.ds;

import akka.actor.ActorRef;

public class ProbeTransaction extends Transaction {

    ProbeTransactionStartParameters startParameters;

    public ProbeTransaction(TransactionId id, DistributedActor owner) {
        this(id, owner, null, null);
    }

    public ProbeTransaction(TransactionId id, DistributedActor owner, ProbeMsg initialMsg, ActorRef targetActor) {
        super(id, owner);
        if (initialMsg == null || targetActor == null) {
            this.startParameters = null;
        } else {
            this.startParameters = new ProbeTransactionStartParameters();
            this.startParameters.initialMsg = initialMsg;
            this.startParameters.targetActor = targetActor;
        }
    }

    /**
     * Represents a test message for the ProbeTransaction, containing a string content.
     */
    public static class ProbeMsg extends Msg {
        public final String content;

        public ProbeMsg(TransactionId transactionId, EpochPair epochPair, ActorRef sender, String content) {
            super(transactionId, epochPair, sender);
            this.content = content;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof ProbeMsg) {
                ProbeMsg other = (ProbeMsg) obj;
                return super.equals(other) && this.content.equals(other.content);
            }
            return false;
        }
    }

    /**
     * Represents the parameters for starting a ProbeTransaction asynchronously.
     */
    public static class ProbeTransactionStartParameters implements StartParameters {
        Msg initialMsg;
        ActorRef targetActor;
    }

    @Override
    public String getState() {
        return "OK";
    }

    @Override
    public void computeState(Msg msg) {
        if (msg instanceof ProbeMsg) {
            ProbeMsg testMsg = (ProbeMsg) msg;
            // if(testMsg.content.equals("start")) {
            //     owner.unicast(new ProbeMsg(testMsg.transactionId, testMsg.epochPair, owner.getSelf(), "ack"),
            // testMsg.sender);
            //     owner.debug("Start Test Transaction " + this.id);
            // }else
            if (testMsg.content.equals("ack")) {
                owner.unicast(
                        new ProbeMsg(testMsg.transactionId, testMsg.epochPair, owner.getSelf(), "done"),
                        testMsg.sender);
                owner.debug("Ack Test Transaction " + this.getId());
            } else if (testMsg.content.equals("done")) {
                owner.debug("Done Test Transaction " + this.getId());
                owner.onTransactionComplete(this);
            }
        }
    }

    public void start() {
        if (startParameters == null) {
            owner.debug("No start parameters provided for ProbeTransaction. Transaction will not be started.");
            throw new IllegalStateException("No start parameters provided for ProbeTransaction.");
        }
        owner.debug("Start Test Transaction " + this.getId());
        ProbeMsg initialMsg = new ProbeMsg(
                this.startParameters.initialMsg.transactionId,
                this.startParameters.initialMsg.epochPair,
                owner.getSelf(),
                "ack");
        owner.unicast(initialMsg, startParameters.targetActor);
    }

    public void setStartParameters(ProbeMsg initialMsg, ActorRef targetActor) {
        if (initialMsg == null || targetActor == null) {
            throw new IllegalArgumentException("Initial message and target actor cannot be null.");
        }
        this.startParameters = new ProbeTransactionStartParameters();
        this.startParameters.initialMsg = initialMsg;
        this.startParameters.targetActor = targetActor;
    }
}
