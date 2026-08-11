package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.ds.Transaction.TransactionId;
import it.unitn.ds.TestTransaction.TestMsg;

import java.util.Optional;
import java.util.LinkedList;
import java.util.Map;

public class Replica extends AbstractReplica implements DistributedActor {
    
    private Map<Integer, ActorRef> groupOfReplicas;
    private LinkedList<Transaction> activeTransactions;

    public static class ReadMsg extends Msg {
        public final int index;

        public ReadMsg(TransactionId transactionId, EpochPair epochPair, ActorRef sender, int index) {
            super(transactionId, epochPair, sender);
            this.index = index;
        }
    }
    

    int positions[];
    int coordinatorID;

    ///////////// For chashing ////////////
    private enum CrashStatus {
        NONE,
        PENDING,
        CRASHED
    };
    private CrashStatus replicaStatus;
    private int crashCount;
    private AbstractReplica.Crash pendingCrash;
    ////////////////////////////////////////////

    public Replica(int id) {
        this(id, AbstractReplica.MIN_LATENCY, AbstractReplica.MAX_LATENCY, AbstractReplica.COORDINATOR_BEAT_INTERVAL, Optional.empty());
    }

    public Replica(int id, int minLatency, int maxLatency, int coordinatorBeatInterval, Optional<ActorRef> listener) {
        super(id, minLatency, maxLatency, coordinatorBeatInterval, listener);
        positions = new int[AbstractReplica.POSITIONS_LIST_LENGTH];
        this.pendingCrash = null;
        this.replicaStatus = CrashStatus.NONE;
        this.crashCount = 0;
        activeTransactions = new LinkedList<>();
    }

    public static Props props(int id, int minLatency, int maxLatency, int coordinatorBeatInterval) {
        return Props.create(Replica.class, () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval, Optional.empty()));
    }

    // Props method for automated tests
    public static Props propsWithListener(int id, int minLatency, int maxLatency, int coordinatorBeatInterval, ActorRef listener) {
        return Props.create(Replica.class, () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval, Optional.ofNullable(listener)));
    }
    

    ///////////// Sending helpers ////////////
    // public abstract class Msg implements Serializable {};

    /**
     * Broadcasts a message to all replicas in the group.
     * @param msg The message to be broadcasted.
     * @param includeSelf A boolean flag indicating whether to include the sender replica in the broadcast. If true, the message will also be sent to the sender replica; if false, it will be excluded.
     * 
     * @apiNote This method will be empowered in the future and will be able to send messages using total ordering
     */
    public void broadcast(Msg msg, boolean includeSelf){
        if(this.replicaStatus == CrashStatus.CRASHED){
            return;
        }

        ActorRef target;
        for (Map.Entry<Integer, ActorRef> entry : groupOfReplicas.entrySet()) {
            target = entry.getValue();
            if(target != getSelf() || includeSelf){
                target.tell(msg, getSelf());
            }
        }
    }

    /**
     * Broadcasts a message to all replicas in the group except itself.
     * 
     * @param msg The message to be broadcasted.
     */
    public void broadcast(Msg msg){
        broadcast(msg, false);
    }

    /**
     * Sends a message to a specific replica.
     * @param msg The message to be sent.
     * @param target The replica to which the message will be sent.
     * @apiNote This method will be empowered in the future and will be able to send messages using total ordering
     */
    public void unicast(Msg msg, ActorRef target){
        if(this.replicaStatus == CrashStatus.CRASHED){
            return;
        }
        
        if(target == getSelf()){
            return;
        }
        target.tell(msg, getSelf());
    }
    ////////////////////////////////////////////

    @Override
    public void initSystem(InitSystem sysInit) {
        this.groupOfReplicas = sysInit.group; 
        this.coordinatorID = sysInit.coordinator_id;
        log("Initialized with group of replicas: " + getSystemNumberOfActors() + " replicas, coordinator ID: " + coordinatorID);
    }

    @Override
    public int getSystemNumberOfActors() {
        return this.groupOfReplicas.size();
    }

    @Override
    public void crash(AbstractReplica.Crash how_to_crash) {
        if(how_to_crash.type == AbstractReplica.Crash.Type.Now){
            this.replicaStatus = CrashStatus.CRASHED;
            log("Replica crashed immediately due to " + how_to_crash.type + " crash.");
            return;
        }
        this.pendingCrash = how_to_crash;
        this.crashCount = 0;
        this.replicaStatus = CrashStatus.PENDING;
    }

    @Override
    public void onTransactionComplete(Transaction transaction) {
        activeTransactions.remove(transaction);
    }

    @Override
    public final Receive createReceive() {
        return createBaseReceiveBuilder()
                .match(ReadMsg.class, this::onReadMsg)
                .match(TestMsg.class, this::onTestMsg)
                .build();
    }

    @Override
    public void onMessage(Msg msg) {
        if(this.replicaStatus != CrashStatus.NONE){
            crashCount++;
            return;
        }
        for(Transaction transaction : activeTransactions){
            if(transaction.id.equals(msg.transactionId)){
                transaction.computeState(msg);
                return;
            }
        }
    }

    public void onReadMsg(ReadMsg msg) {
        onMessage(msg);
        unicast(new Client.ReadResult(true, msg.index, positions[msg.index], this.id), msg.sender);
    }
    

    /// For testing
    public void onTestMsg(TestMsg msg) {
        if(msg.content.equals("start")){
            TransactionId tId = new TransactionId(this.getSelf(), this.activeTransactions.size() + 1);
            TestTransaction transaction = new TestTransaction(tId.transactionId, this);
            this.activeTransactions.add(transaction);
        }
        onMessage(msg);
    }

    /**
     * This callback method is invoked whenever a message is recieved by the replica and the parameter allows to differentiate the type of message.
     * The callback then checks if the replica is in a pending crash state and if the type of message matches the pending crash type. 
     * If so, it increments the crash count and checks if it has reached the threshold for crashing. 
     * If the threshold is met, the replica's status is updated to CRASHED.
     * @param crashType Enum representing the type of message received, used to determine if the replica should crash and if to increment the crash count.
     */
    void updateCrashStatusCallback(AbstractReplica.Crash.Type crashType) {
        if (this.replicaStatus == CrashStatus.PENDING && this.pendingCrash.type == crashType) {
            this.crashCount++;
            if (this.crashCount >= this.pendingCrash.after_n_messages_of_type) {
                this.replicaStatus = CrashStatus.CRASHED;
                log("Replica crashed due to " + crashType + " crash.");
            }
        }
    }
}
