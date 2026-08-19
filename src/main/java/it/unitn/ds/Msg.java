package it.unitn.ds;

import akka.actor.ActorRef;
import it.unitn.ds.Transaction.TransactionId;
import java.io.Serializable;
import java.util.Objects;

public abstract class Msg implements Serializable {
    public final TransactionId transactionId;
    public final EpochPair epochPair;

    public final ActorRef sender;

    public Msg(TransactionId transactionId, EpochPair epochPair, ActorRef sender) {
        this.transactionId = transactionId;
        this.epochPair = epochPair;
        this.sender = sender;
    }

    @Override
    public String toString() {
        return "Msg{tId="
                + transactionId + ", epochPair="
                + epochPair + ", sender="
                + (sender == null ? "none" : sender.path()) + '}';
    }

    @Override
    public int hashCode() {
        int result = transactionId.hashCode();
        result = 31 * result + (epochPair != null ? epochPair.hashCode() : 0);
        result = 31 * result + (sender != null ? Objects.hashCode(sender) : 0);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        Msg other = (Msg) obj;
        return this.transactionId.equals(other.transactionId)
                && (this.epochPair != null ? this.epochPair.equals(other.epochPair) : other.epochPair == null)
                && (this.sender != null ? Objects.equals(this.sender, other.sender) : other.sender == null);
    }
}
;
