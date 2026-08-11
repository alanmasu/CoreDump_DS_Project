package it.unitn.ds;

public class TestTransaction extends Transaction {

    public TestTransaction(int id, DistributedActor owner) {
        super(id, owner);
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
