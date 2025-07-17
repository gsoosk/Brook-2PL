package edgelab.retryFreeDB.repo.concurrencyControl;

import edgelab.retryFreeDB.benchmark.Performance;
import edgelab.retryFreeDB.repo.storage.DTO.DBData;
import lombok.Getter;
import lombok.Setter;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class DBTransaction {
    private static final long DEADLOCK_TIMEOUT = 1_000_000;
    @Getter
    private String timestamp;
    @Getter
    private AtomicBoolean abort = new AtomicBoolean(false);
    private final AtomicBoolean rollBacked = new AtomicBoolean(false);
    @Getter
    @Setter
    private Connection connection;

    @Getter
    @Setter
    private boolean interactiveTransaction;


    @Getter
    private long randomPriority;


    @Getter
    private long waitingTime;
    @Getter
    private long ioTime;
    @Getter
    private long lockingTime;
    @Getter
    private long retiringTime;
    @Getter
    private long initiationTime;
    @Getter
    private long unlockingTime;
    @Getter
    private long committingTime;
    @Getter
    private long waitingForOthersTime;
    @Getter
    private long rollingBackTime;

    @Getter
    private long startTime;
    private long previousRetireStart = -1;
    private long previousWaitStart = -1;
    private long previousIOStart = -1;
    private long previousLockStart = -1;
    private long previousUnlockStart = -1;
    private long previousCommitStart = -1;
    private long previousWaitingForOthersStart = -1;
    private long previousRollBackStart = -1;

    @Getter
    private final Set<String> resources = ConcurrentHashMap.newKeySet();
    private final AtomicInteger commitSemaphore;

    @Getter
    private final Map<String, Optional<Map<String, String>>> WAL;

    @Getter
    private final Set<String> OCCReadSet;
    @Getter
    private final Map<String, DBData> OCCWriteSet;

    private List<String> OCCtoLock =  null;

    public List<String> getOCCtoLock() {
        if (this.OCCtoLock == null) {
            Set<String> toLockSet = new HashSet<>(this.getOCCReadSet());
            toLockSet.addAll(this.getOCCWriteSet().keySet());
            this.OCCtoLock = new ArrayList<>(toLockSet);
            this.OCCtoLock.sort(String::compareTo);
        }
        return this.OCCtoLock;
    }

    @Getter
    private final ConcurrentLinkedQueue<DBTransaction> IC3DepQueue = new ConcurrentLinkedQueue<DBTransaction>();

    public void addIC3Dependency(DBTransaction tx) {
        IC3DepQueue.add(tx);
    }

    @Getter
    private final ConcurrentLinkedQueue<IC3Piece> IC3Pieces = new ConcurrentLinkedQueue<IC3Piece>();

    @Setter
    @Getter
    private IC3Piece activeIC3Piece;

    private final AtomicBoolean committed = new AtomicBoolean(false);

    public void commit() {
        committed.set(true);
    }

    public boolean isCommitted() {
        return committed.get();
    }


    private final AtomicBoolean OCCValidated;
    private final AtomicBoolean OCCFinished;
    public Set<DBTransaction> OCCInitTransactions;
    public void OCCValidate() {
        OCCValidated.set(true);
    }
    public boolean isOCCValidated() {
        return OCCValidated.get();
    }

    public void OCCFinish() {
        OCCFinished.set(true);
    }

    public boolean isOCCFinished() {
        return OCCFinished.get();
    }
    public DBTransaction(String timestamp) {

        this.timestamp = timestamp;
        this.connection = null;
        commitSemaphore = new AtomicInteger(0);
        this.waitingTime = 0;
        this.ioTime = 0;
        this.lockingTime = 0;
        this.retiringTime = 0;
        this.initiationTime = 0;
        this.unlockingTime = 0;
        this.committingTime = 0;
        this.waitingForOthersTime = 0;
        this.rollingBackTime = 0;
        this.WAL = new ConcurrentHashMap<>();
        this.OCCReadSet = ConcurrentHashMap.newKeySet();
        this.OCCWriteSet = new ConcurrentHashMap<>();
        this.OCCValidated = new AtomicBoolean(false);
        this.OCCFinished = new AtomicBoolean(false);
        this.randomPriority = Performance.random.nextLong();
        this.startTime = System.nanoTime();
    }

    @Override
    public String toString() {
        return timestamp;
    }

    public void setAbort() {
        this.abort.set(true);
    }
    public boolean isAbort() {return this.abort.get();}


    public void addResource(String resource) {
        resources.add(resource);
    }

    public void clearResources() {
        resources.clear();
    }

    public void removeResource(String resource) {
        resources.remove(resource);
    }

    public void incCommitSemaphore() {
        commitSemaphore.incrementAndGet();
    }
    public void decCommitSemaphore() {
        commitSemaphore.decrementAndGet();
    }

    public Long getTimestampValue() {
        return Long.parseLong(timestamp);
    }


    private boolean canCommit() {
        return commitSemaphore.get() == 0;
    }

    public void waitForCommit() throws Exception {
        while (!this.canCommit()) {
            if (isAbort())
                throw new Exception("transaction already aborted");
            Thread.sleep(0, 1000);
        }
    }

    public void startWaiting() {
        previousWaitStart = System.nanoTime();
    }

    public void startIO() { previousIOStart = System.nanoTime(); }

    public void startLocking() {previousLockStart = System.nanoTime();}

    public void startRetireLock() {previousRetireStart = System.nanoTime();}

    public void startUnlocking() {previousUnlockStart = System.nanoTime();}

    public void startCommitting() {previousCommitStart = System.nanoTime();}

    public void startWaitingForOthers() {previousWaitingForOthersStart = System.nanoTime();}

    public void startRollingBack() {
        rollBacked.set(true);
        previousRollBackStart = System.nanoTime();
    }


    public void wakesUp() {
        if (previousWaitStart != -1)
            this.waitingTime += (System.nanoTime() - previousWaitStart);
    }

    public void finishIO() {
        if (previousIOStart != -1)
            this.ioTime += (System.nanoTime() - previousIOStart);
    }

    public void finishLocking() {
        if (previousLockStart != -1)
            this.lockingTime += (System.nanoTime() - previousLockStart);
    }

    public void finishRetireLock() {
        if (previousRetireStart != -1)
            this.retiringTime += (System.nanoTime() - previousRetireStart);
    }

    public void finishInitiation(long startTime) {
        this.initiationTime += (System.nanoTime() - startTime);
    }

    public void finishUnlocking() {
        if (previousUnlockStart != -1)
            this.unlockingTime += (System.nanoTime() - previousUnlockStart);
    }

    public void finishCommitting() {
        if (previousCommitStart != -1)
            this.committingTime += (System.nanoTime() - previousCommitStart);

        commit();
    }

    public void finishWaitingForOthers() {
        if (previousWaitingForOthersStart != -1)
            this.waitingForOthersTime += (System.nanoTime() - previousWaitingForOthersStart);
    }
    public void finishRollingBack() {
        if (previousRollBackStart != -1)
            this.rollingBackTime += (System.nanoTime() - previousRollBackStart);
    }
    public void appendWAL(String key, Map<String, String> initValue) {
        if (!this.WAL.containsKey(key))
            this.WAL.put(key, Optional.ofNullable(initValue));
    }


    public boolean isRolledBack() {
        return rollBacked.get();
    }

    public boolean isDeadLock() {
        return System.nanoTime() - startTime > DEADLOCK_TIMEOUT;
    }

    public void addOCCRead(String key) {
        this.OCCReadSet.add(key);
    }


    public void addOCCWrite(String key, DBData value) {
        this.OCCReadSet.add(key);
        this.OCCWriteSet.put(key, value);
    }

    public void addIC3Piece(IC3Piece piece) {
        this.IC3Pieces.add(piece);
    }
}
