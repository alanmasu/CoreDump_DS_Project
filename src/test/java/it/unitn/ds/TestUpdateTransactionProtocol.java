package it.unitn.ds;

import static org.junit.jupiter.api.Assertions.assertEquals;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.TestActorRef;
import it.unitn.ds.Transaction.TransactionId;
import it.unitn.ds.UpdateTransaction.UpdateTimeoutMsg;
import it.unitn.ds.UpdateTransaction.UpdateTransactionState;
import it.unitn.ds.UpdateTransaction.WriteOkMsg;
import it.unitn.ds.UpdateTransaction.WriteOkTimeoutMsg;
import org.junit.jupiter.api.Test;

class TestUpdateTransactionProtocol {

    @Test
    void coordinatorAllocatesUniquePairs() {
        ActorSystem system = ActorSystem.create("updatePairAllocation");
        try {
            TestActorRef<Replica> replicaRef = TestActorRef.create(system, Replica.props(0, 1, 1, 100));
            Replica replica = replicaRef.underlyingActor();
            replica.setCoordinatorID(0);

            assertEquals(new EpochPair(0, 1), replica.reserveNextUpdateEpochPair());
            assertEquals(new EpochPair(0, 2), replica.reserveNextUpdateEpochPair());
            assertEquals(new EpochPair(0, 3), replica.reserveNextUpdateEpochPair());

            replica.setEpochPair(new EpochPair(1, 4));
            assertEquals(new EpochPair(1, 5), replica.reserveNextUpdateEpochPair());
        } finally {
            system.terminate();
        }
    }

    @Test
    void historyUsesTheWriteOkPair() {
        ActorSystem system = ActorSystem.create("updateHistoryPair");
        try {
            TestActorRef<Replica> replicaRef = TestActorRef.create(system, Replica.props(0, 1, 1, 100));
            Replica replica = replicaRef.underlyingActor();
            replica.setCoordinatorID(0);
            TransactionId transactionId = new TransactionId(replicaRef, 1);
            EpochPair committedPair = new EpochPair(3, 7);
            UpdateTransaction transaction =
                    new UpdateTransaction(transactionId, replica, replica.getEpochPair(), 0, 42, null, null);

            transaction.termination(replica, new WriteOkMsg(transactionId, committedPair, replicaRef, 0, 42));

            assertEquals(committedPair, replica.getEpochPair());
            assertEquals(transaction, replica.getUpdateHistory().get(committedPair));
        } finally {
            system.terminate();
        }
    }

    @Test
    void participantTimeoutsEnterElectionWait() {
        ActorSystem system = ActorSystem.create("updateTimeoutHandling");
        try {
            TestActorRef<Replica> replicaRef = TestActorRef.create(system, Replica.props(1, 1, 1, 100));
            Replica replica = replicaRef.underlyingActor();
            replica.setCoordinatorID(0);
            TransactionId transactionId = new TransactionId(replicaRef, 1);
            EpochPair updatePair = new EpochPair(0, 1);

            UpdateTransaction waitingForUpdate =
                    new UpdateTransaction(transactionId, replica, updatePair, 0, 42, ActorRef.noSender(), null);
            waitingForUpdate.state = UpdateTransactionState.WAITING_UPDATE;
            waitingForUpdate.computeState(new UpdateTimeoutMsg(transactionId, updatePair, replicaRef));
            assertEquals(UpdateTransactionState.WAITING_ELECTION.toString(), waitingForUpdate.getState());

            UpdateTransaction waitingForWriteOk = new UpdateTransaction(
                    new TransactionId(replicaRef, 2), replica, updatePair, 0, 42, ActorRef.noSender(), null);
            waitingForWriteOk.state = UpdateTransactionState.WAITING_WRITEOK;
            waitingForWriteOk.computeState(new WriteOkTimeoutMsg(waitingForWriteOk.getId(), updatePair, replicaRef));
            assertEquals(UpdateTransactionState.WAITING_ELECTION.toString(), waitingForWriteOk.getState());
        } finally {
            system.terminate();
        }
    }
}
