package it.unitn.ds;

import scala.concurrent.duration.Duration;
import java.util.concurrent.TimeUnit;




/**
 * Manages heartbeat behavior for a Replica.
 * 
 * When the owner is the coordinator, it periodically broadcasts HEARTBEAT
 * messages. When the owner is a follower, it watches for those messages and
 * requests an election if the coordinator stops sending them, thus detecting
 * a crash.
 */
public final class HeartbeatTransaction extends Transaction {

    
    private static final int MISSED_HEARTBEATS_BEFORE_FAILURE = 3;

    // Finite state machine states
    private enum State {
        STOPPED, 
        COORDINATOR,
        WATCHING,
        ELECTION_REQUESTED
    }

    /**
     * Internal event sent by the Akka Scheduler to the coordinator Replica.
     * 
     * It contains no data: receiving the event simply means that it is time to
     * broadcast the next heartbeat.
     */
    public static final class HeartbeatTick extends Msg {}

    /**
     * Network Message broadcast by the coordinator to follower Replicas.
     */
    public static final class Heartbeat extends Msg {
        
        public final int coordinatorId;

        public Heartbeat(int coordinatorId) {
            this.coordinatorId = coordinatorId;
        }
    }

    /**
     * Internal event sent by the Akka Scheduler when a follower's watchdog expires.
     * 
     * The version identifies the watchdog that created this event. It allows the
     * transaction to distinguish the active timeout from an older stale timeout.
     */
    public static final class WatchdogExpired extends Msg {

        public final long watchdogVersion;

        public WatchdogExpired(long watchdogVersion) {
            this.watchdogVersion = watchdogVersion;
        }
    }

    // The transaction always starts inactive
    private State state;

    /**
     * The Replica whose heartbeat behavior this transaction manages.
     * 
     * Transaction.owner references the same object, but its type is AbstractActor.
     * Keeping this typed reference gives us access to Replica-specific data.
     */
    private final Replica replica;

    /**
     * Identifies the follower's currently active watchdog.
     * 
     * It starts at zero and increases whenever a new watchdog is scheduled.
     */
    private long watchdogVersion;


    public HeartbeatTransaction(int transactionId, Replica owner) {
        super(transactionId, owner);

        this.replica = owner;
        this.state = State.STOPPED;
        this.watchdogVersion = 0;
    }

    /**
     * Starts heartbeat behavior according to the role of the owning Replica.
     * 
     * Repeated calls are ignored so that we cannot accidentally create multiple
     * heartbeat timers or watchdogs for the same transactions.
     */
    public void start() {
        if (state != State.STOPPED) {
            return;
        }

        if (replica.id == replica.coordinatorID) {
            state = State.COORDINATOR;
            scheduleHeartbeatTick();
        } else {
            state = State.WATCHING;
            restartWatchdog();
        }
    }

    /**
     * Gives the coordinator several opportunities to send a heartbeat before
     * the follower considers it crashed.
     */
    private long getWatchdogTimeoutMillis() {
        long heartbeatAllowance = 
            (long) MISSED_HEARTBEATS_BEFORE_FAILURE
                * replica.getCoordinatorBeatInterval();
        
        return heartbeatAllowance + replica.getMaxLatencyPlusTolerance();
    }


    /**
     * Schedules one HeartbeatTick for the coordinator Replica.
     * 
     * The tick is sent to the Replica's mailbox; HeartbeatTransaction is not
     * itself an actor and therefore has no mailbox.
     */
    private void scheduleHeartbeatTick() {
        timeout = replica.getContext()
                         .system()
                         .scheduler()
                         .scheduleOnce(
                            Duration.create(
                                replica.getCoordinatorBeatInterval(),
                                TimeUnit.MILLISECONDS
                            ), 
                            replica.getSelf(),
                            new HeartbeatTick(),
                            replica.getContext().system().dispatcher(),
                            replica.getSelf()    
                        ); 
    }


    /**
     * Cancels the old watchdog and schedules a new version.
     */
    private void restartWatchdog() {
        if (timeout != null) {
            timeout.cancel();
        }

        watchdogVersion++;

        timeout = replica.getContext()
                         .system()
                         .scheduler()
                         .scheduleOnce(
                            Duration.create(
                                getWatchdogTimeoutMillis(),
                                TimeUnit.MILLISECONDS      
                            ),
                            replica.getSelf(),
                            new WatchdogExpired(watchdogVersion),
                            replica.getContext().system().dispatcher(),
                            replica.getSelf()
                         );
    }



    @Override
    public String getState() {
        return state.name();
    }

    @Override
    public void computeState(Msg msg) {
        throw new UnsupportedOperationException(
            "HeartbeatTransaction.computeState is not implemented yet."
        );
    }
}