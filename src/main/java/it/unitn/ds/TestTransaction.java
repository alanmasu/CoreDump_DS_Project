package it.unitn.ds;

import akka.actor.ActorRef;

public class TestTransaction extends Transaction {

    TestTransactionStartParameters startParameters;

    public TestTransaction(TransactionId id, DistributedActor owner) {
        this(id, owner, null , null);
    }
    
    public TestTransaction(TransactionId id, DistributedActor owner, TestMsg initialMsg, ActorRef targetActor) {
        super(id, owner);
        if(initialMsg == null || targetActor == null) {
            this.startParameters = null;
        } else {
            this.startParameters = new TestTransactionStartParameters();
            this.startParameters.initialMsg = initialMsg;
            this.startParameters.targetActor = targetActor;
        }
    }

    /**
     * Represents a test message for the TestTransaction, containing a string content.
     */
    public static class TestMsg extends Msg {
        public final String content;

        public TestMsg(TransactionId transactionId, EpochPair epochPair, ActorRef sender, String content) {
            super(transactionId, epochPair, sender);
            this.content = content;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof TestMsg) {
                TestMsg other = (TestMsg) obj;
                return super.equals(other) && this.content.equals(other.content);
            }
            return false;
        }
    }

    /**
     * Represents the parameters for starting a TestTransaction asynchronously.
     */
    public static class TestTransactionStartParameters implements StartParameters {
        Msg initialMsg;
        ActorRef targetActor;
    }

    @Override
    public String getState(){
        return "OK";
    }
    
    @Override
    public void computeState(Msg msg) {
        if (msg instanceof TestMsg) {
            TestMsg testMsg = (TestMsg) msg;
            // if(testMsg.content.equals("start")) {
            //     owner.unicast(new TestMsg(testMsg.transactionId, testMsg.epochPair, owner.getSelf(), "ack"), testMsg.sender);
            //     owner.debug("Start Test Transaction " + this.id);
            // }else 
                if(testMsg.content.equals("ack")) {
                owner.unicast(new TestMsg(testMsg.transactionId, testMsg.epochPair, owner.getSelf(), "done"), testMsg.sender);
                owner.debug("Ack Test Transaction " + this.getId());
            }else if(testMsg.content.equals("done")) {
                owner.debug("Done Test Transaction " + this.getId());
                owner.onTransactionComplete(this);
            }
        }
    }

    public void start() {
        if (startParameters == null) {
            owner.debug("No start parameters provided for TestTransaction. Transaction will not be started.");
            throw new IllegalStateException("No start parameters provided for TestTransaction.");
        }
        owner.debug("Start Test Transaction " + this.getId());
        TestMsg initialMsg = new TestMsg(   this.startParameters.initialMsg.transactionId, 
                                            this.startParameters.initialMsg.epochPair, 
                                            owner.getSelf(), 
                                            "ack");
        owner.unicast(initialMsg, startParameters.targetActor);
    }

    @Override
    public void setStartParameters(StartParameters startParameters) {
        if (startParameters instanceof TestTransactionStartParameters) {
            this.startParameters = (TestTransactionStartParameters) startParameters;
        } else {
            throw new IllegalArgumentException("Invalid start parameters for TestTransaction.");
        }
    }
}
