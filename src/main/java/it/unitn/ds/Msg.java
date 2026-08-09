package it.unitn.ds;
import java.io.Serializable;

import akka.actor.ActorRef;


public abstract class Msg implements Serializable {
    public final int ownerId;
    public final int ownerTransactionId;
    public final EpochPair epochPair;

    public final ActorRef sender;
    

    public Msg(int ownerId, int ownerTransactionId, EpochPair epochPair, ActorRef sender) {
        this.ownerId = ownerId;
        this.ownerTransactionId = ownerTransactionId;
        this.epochPair = epochPair;
        this.sender = sender;
    }

    @Override
    public String toString() {
        return "Msg{" +
                "ownerId=" + ownerId +
                ", ownerTransactionId=" + ownerTransactionId +
                ", epochPair=" + epochPair +
                ", sender=" + sender.path().name() +
                '}';
    }

    @Override
    public int hashCode() {
        int result = Integer.hashCode(ownerId);
        result = 31 * result + Integer.hashCode(ownerTransactionId);
        result = 31 * result + (epochPair != null ? epochPair.hashCode() : 0);
        result = 31 * result + (sender != null ? sender.path().name().hashCode() : 0);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        Msg other = (Msg) obj;
        return this.ownerId == other.ownerId &&
               this.ownerTransactionId == other.ownerTransactionId &&
               (this.epochPair != null ? this.epochPair.equals(other.epochPair) : other.epochPair == null) &&
               (this.sender != null ? this.sender.path().name().equals(other.sender.path().name()) : other.sender == null);
    }

};