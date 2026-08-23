package it.unitn.ds;

import scala.concurrent.duration.Duration;
import java.util.concurrent.TimeUnit;
import it.unitn.ds.Transaction.TransactionId;

import akka.actor.ActorRef;

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
     * Internal scheduler message telling the coordinator to broadcast its next
     * heartbeat.
     *
     * <p>This message is delivered only to the owning Replica's mailbox. It is not
     * sent through the network channel. Its transaction ID identifies the
     * heartbeat transaction for the current coordinator term.</p>
     */
    public static final class HeartbeatTickMsg extends Msg {
        

        /**
         * Creates a heartbeat tick for the given heartbeat transaction.
         *
         * @param transactionId identifier of the current heartbeat transaction
         * @param epochPair epoch pair associated with the heartbeat transaction
         * @param sender coordinator Replica that scheduled the tick
         */
        public HeartbeatTickMsg(
            TransactionId transactionId,
            EpochPair epochPair,
            ActorRef sender
        ) {
            super(transactionId, epochPair, sender);
        }
    }


    /**
     * Network message broadcast by the coordinator to announce that it is alive.
     *
     * <p>Every replica participating in the same coordinator term uses the same
     * transaction ID. Followers accept the message only when
     * {@code coordinatorId} matches their currently expected coordinator.</p>
     */
    public static final class HeartbeatMsg extends Msg {
        
        public final int coordinatorId;

        /**
         * Creates a heartbeat sent by the current coordinator.
         *
         * @param transactionId identifier of the current heartbeat transaction
         * @param epochPair epoch pair associated with the heartbeat transaction
         * @param sender coordinator Replica broadcasting the heartbeat
         * @param coordinatorId numeric identifier of the current coordinator
         */
        public HeartbeatMsg(
            TransactionId transactionId,
            EpochPair epochPair,
            ActorRef sender,
            int coordinatorId
        ) {
            super(transactionId, epochPair, sender);
            this.coordinatorId = coordinatorId;
        }
    }


    /**
     * Internal scheduler message indicating that a follower's watchdog expired.
     *
     * <p>The transaction ID identifies the coordinator-term transaction, while
     * {@code watchdogVersion} identifies one particular watchdog generation.
     * This distinction allows the transaction to reject an older timeout message
     * that entered the mailbox before its timer was cancelled.</p>
     */
    public static final class WatchdogExpiredMsg extends Msg {

        public final long watchdogVersion;

        /**
         * Creates a watchdog-expiration message.
         *
         * @param transactionId identifier of the current heartbeat transaction
         * @param epochPair epoch pair associated with the heartbeat transaction
         * @param sender follower Replica that scheduled the watchdog
         * @param watchdogVersion generation of the watchdog that expired
         */
        public WatchdogExpiredMsg(
            TransactionId transactionId,
            EpochPair epochPair,
            ActorRef sender,
            long watchdogVersion
        ) {
            super(transactionId, epochPair, sender);
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


    /**
     * Creates a heartbeat transaction owned by a replica.
     *
     * @param transactionId coordinator-term transaction
     * @param owner replica that owns the transaction
     * @param startEpochPair epoch pair associated with transaction startup
     */
    public HeartbeatTransaction(TransactionId transactionId, Replica owner, EpochPair startEpochPair) {
        super(transactionId, owner, startEpochPair);

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

        if (replica.id == replica.getCoordinatorID()) {
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
     * The tick carries this transaction's ID so the Replica can route it back to
     * the correct heartbeat transaction. It is delivered locally to the Replica's
     * mailbox and never crosses the network channel.
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
                            new HeartbeatTickMsg(
                                getId(),
                                startEpochPair,
                                replica.getSelf()
                            ),
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
                            new WatchdogExpiredMsg(
                                getId(),
                                startEpochPair,
                                replica.getSelf(),
                                watchdogVersion
                            ),
                            replica.getContext().system().dispatcher(),
                            replica.getSelf()
                         );
    }

    /**
     * Handles the coordinator's scheduled heartbeat event.
     *
     * A tick received in another state is stale and must not produce a heartbeat.
     */
    private void handleHeartbeatTick() {
        if (state != State.COORDINATOR) {
            return;
        }

        replica.broadcast(
            new HeartbeatMsg(
                getId(),
                startEpochPair,
                replica.getSelf(),
                replica.id
            )
        );
        scheduleHeartbeatTick();
    }


    /**
     * Handles a heartbeat received by a follower.
     *
     * Only a heartbeat from the currently expected coordinator can reset the
     * watchdog. Heartbeats received in other states are irrelevant.
     */
    private void handleHeartbeat(HeartbeatMsg heartbeat) {
        if (state != State.WATCHING) {
            return;
        }

        if (heartbeat.coordinatorId != replica.getCoordinatorID()) {
            return;
        }

        restartWatchdog();
    }


    /**
     * Handles the expiration of a follower's watchdog.
     *
     * Only the currently active watchdog can cause a transition. Older timeout messages are stale
     * and must be ignored.
     */
    private void handleWatchdogExpired(WatchdogExpiredMsg expired) {
        if (state != State.WATCHING) {
            return;
        }

        // This is needed to prevent previous expired watchdogs in the mailboxes to cause
        // an undesired election. For example:
        // - Watchdog 4 scheduled
        // - Heartbeat arrives
        // - Watchdog 4 cancelled
        // - Watchdog 5 scheduled
        // - WatchdogExpired(4) was already in the mailbox
        // - This causes an unwanted election if versions are not checked
        if (expired.watchdogVersion != watchdogVersion) {
            return;
        }

        state = State.ELECTION_REQUESTED;

        // The scheduled timeout has fired, so no active scheduled timeout remains.
        timeout = null;


        // TODO: ELECTION_TRANSACTION must be started here once it's implemented
    }

    @Override
    public String getState() {
        return state.name();
    }

    @Override
    public void computeState(Msg msg) {
        if (msg instanceof HeartbeatTickMsg) {
            handleHeartbeatTick();
            return;
        }

        if (msg instanceof HeartbeatMsg) {
            handleHeartbeat((HeartbeatMsg) msg);
            return;
        }

        if (msg instanceof WatchdogExpiredMsg) {
            handleWatchdogExpired((WatchdogExpiredMsg) msg);
            return;
        }

        throw new IllegalArgumentException(
            "Unsupported heartbeat message: "
                + msg.getClass().getSimpleName()
        );
    }
}