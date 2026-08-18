package it.unitn.ds;

import akka.actor.ActorRef;

public class ProbeTransaction extends Transaction {

    /** Probe protocol vocabulary: start -> ack -> done. */
    public static final String MSG_START = "start";

    public static final String MSG_ACK = "ack";
    public static final String MSG_DONE = "done";

    ProbeTransactionStartParameters startParameters;

    public ProbeTransaction(TransactionId id, DistributedActor owner, EpochPair startEpochPair) {
        this(id, owner, startEpochPair, null, null);
    }

    public ProbeTransaction(
            TransactionId id,
            DistributedActor owner,
            EpochPair startEpochPair,
            ProbeMsg initialMsg,
            ActorRef targetActor) {
        super(id, owner, startEpochPair);
        // Left at its default null when either argument is missing; start() reports that.
        if (initialMsg != null && targetActor != null) {
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

        @Override
        public int hashCode() {
            int result = super.hashCode();
            result = 31 * result + content.hashCode();
            return result;
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
            // if(testMsg.content.equals(MSG_START)) {
            //     owner.unicast(new ProbeMsg(testMsg.transactionId, testMsg.epochPair, owner.getSelf(), MSG_ACK),
            // testMsg.sender);
            //     owner.debug("Start Test Transaction " + this.id);
            // }else
            if (MSG_ACK.equals(testMsg.content)) {
                owner.unicast(
                        new ProbeMsg(testMsg.transactionId, testMsg.epochPair, owner.getSelf(), MSG_DONE),
                        testMsg.sender);
                owner.debug("Ack Test Transaction " + this.getId());
            } else if (MSG_DONE.equals(testMsg.content)) {
                owner.debug("Done Test Transaction " + this.getId());
                owner.onTransactionComplete(this);
            }
        }
    }

    @Override
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
                MSG_ACK);
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
