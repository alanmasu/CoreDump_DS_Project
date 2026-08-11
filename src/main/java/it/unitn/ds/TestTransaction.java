package it.unitn.ds;

import akka.actor.ActorRef;

public class TestTransaction extends Transaction {

    public TestTransaction(int id, DistributedActor owner) {
        super(id, owner);
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

    @Override
    public String getState(){
        return "OK";
    }
    
    @Override
    public void computeState(Msg msg) {
        if (msg instanceof TestMsg) {
            TestMsg testMsg = (TestMsg) msg;
            if(testMsg.content.equals("start")) {
                testMsg.sender.tell(new TestMsg(testMsg.transactionId, testMsg.epochPair, owner.getSelf(), "ack"), owner.getSelf());
                owner.debug("Start Test Transaction!");
            }else if(testMsg.content.equals("ack")) {
                testMsg.sender.tell(new TestMsg(testMsg.transactionId, testMsg.epochPair, owner.getSelf(), "done"), owner.getSelf());
                owner.debug("Ack Test Transaction!");
            }else if(testMsg.content.equals("done")) {
                owner.debug("Done Test Transaction!");
                owner.onTransactionComplete(this);
            }
        }
    }

}
