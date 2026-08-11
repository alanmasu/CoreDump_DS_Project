
package it.unitn.ds;
import akka.actor.ActorRef;
import it.unitn.ds.Transaction.TransactionId;

public class TestMsg extends Msg {
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