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
                if(owner instanceof Replica){
                    ((Replica)owner).debug("Started Test Transaction!");
                }
            }else if(testMsg.content.equals("ack")) {
                testMsg.sender.tell(new TestMsg(testMsg.transactionId, testMsg.epochPair, owner.getSelf(), "done"), owner.getSelf());
                if(owner instanceof Replica){
                    ((Replica)owner).debug("Ack Test Transaction!");
                }
            }else if(testMsg.content.equals("done")) {
                if(owner instanceof Replica){
                    ((Replica)owner).debug("Done Test Transaction!");
                }
                owner.onTransactionComplete(this);
            }
        }
    }

}
