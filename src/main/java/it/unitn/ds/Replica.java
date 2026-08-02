package it.unitn.ds;

import java.io.Serializable;
import akka.actor.ActorRef;
import akka.actor.Props;

import java.util.Optional;

import java.util.Map;

public class Replica extends AbstractReplica {
    
    private Map<Integer, ActorRef> groupOfReplicas;

    int positions[];
    int coordinatorID;

    public Replica(int id) {
        this(id, AbstractReplica.MIN_LATENCY, AbstractReplica.MAX_LATENCY, AbstractReplica.COORDINATOR_BEAT_INTERVAL, Optional.empty());
    }

    public Replica(int id, int minLatency, int maxLatency, int coordinatorBeatInterval, Optional<ActorRef> listener) {
        super(id, minLatency, maxLatency, coordinatorBeatInterval, listener);
        positions = new int[AbstractReplica.POSITIONS_LIST_LENGTH];
    }

    public static Props props(int id, int minLatency, int maxLatency, int coordinatorBeatInterval) {
        return Props.create(Replica.class, () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval, Optional.empty()));
    }

    // Props method for automated tests
    public static Props propsWithListener(int id, int minLatency, int maxLatency, int coordinatorBeatInterval, ActorRef listener) {
        return Props.create(Replica.class, () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval, Optional.ofNullable(listener)));
    }
    

    ///////////// Sending helpers ////////////
    public abstract class Msg implements Serializable {};

    /**
     * Broadcasts a message to all replicas in the group except itself.
     * @param msg The message to be broadcasted.
     * 
     * @apiNote This method will be empowered in the future and will be able to send messages using total ordering
     */
    public void broadcast(Msg msg){
        ActorRef target;
        for (Map.Entry<Integer, ActorRef> entry : groupOfReplicas.entrySet()) {
            target = entry.getValue();
            if(target != getSelf()){
                target.tell(msg, getSelf());
            }
        }
    }

    /**
     * Sends a message to a specific replica.
     * @param msg The message to be sent.
     * @param target The replica to which the message will be sent.
     * @apiNote This method will be empowered in the future and 
     */
    public void unicast(Msg msg, ActorRef target){
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
        // TODO: implement
    }

    @Override
    public final Receive createReceive() {
        return createBaseReceiveBuilder()
                // TODO add your message handlers here .match(, )
                .build();
    }

}
