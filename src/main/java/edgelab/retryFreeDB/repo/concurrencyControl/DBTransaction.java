package edgelab.retryFreeDB.repo.concurrencyControl;

import lombok.Getter;
import lombok.Setter;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class DBTransaction {
    @Getter
    private String timestamp;
    @Getter
    private boolean abort = false;
    @Getter
    @Setter
    private Connection connection;


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

    private long previousRetireStart = -1;
    private long previousWaitStart = -1;
    private long previousIOStart = -1;
    private long previousLockStart = -1;
    private long previousUnlockStart = -1;
    private long previousCommitStart = -1;
    private long previousWaitingForOthersStart = -1;

    @Getter
    private final Set<String> resources = ConcurrentHashMap.newKeySet();
    private final AtomicInteger commitSemaphore;

    private List<byte[]> WAL;
    public DBTransaction(String timestamp) {

        this.timestamp = timestamp;
        this.abort = false;
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
        this.WAL = new ArrayList<>();
    }

    @Override
    public String toString() {
        return timestamp;
    }

    public void setAbort() {
        this.abort = true;
    }


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
            Thread.sleep(1);

        }
    }

    public void startWaiting() {
        previousWaitStart = System.currentTimeMillis();
    }

    public void startIO() { previousIOStart = System.currentTimeMillis(); }

    public void startLocking() {previousLockStart = System.currentTimeMillis();}

    public void startRetireLock() {previousRetireStart = System.currentTimeMillis();}

    public void startUnlocking() {previousUnlockStart = System.currentTimeMillis();}

    public void startCommitting() {previousCommitStart = System.currentTimeMillis();}

    public void startWaitingForOthers() {previousWaitingForOthersStart = System.currentTimeMillis();}


    public void wakesUp() {
        if (previousWaitStart != -1)
            this.waitingTime += (System.currentTimeMillis() - previousWaitStart);
    }

    public void finishIO() {
        if (previousIOStart != -1)
            this.ioTime += (System.currentTimeMillis() - previousIOStart);
    }

    public void finishLocking() {
        if (previousLockStart != -1)
            this.lockingTime += (System.currentTimeMillis() - previousLockStart);
    }

    public void finishRetireLock() {
        if (previousRetireStart != -1)
            this.retiringTime += (System.currentTimeMillis() - previousRetireStart);
    }

    public void finishInitiation(long startTime) {
        this.initiationTime += (System.currentTimeMillis() - startTime);
    }

    public void finishUnlocking() {
        if (previousUnlockStart != -1)
            this.unlockingTime += (System.currentTimeMillis() - previousUnlockStart);
    }

    public void finishCommitting() {
        if (previousCommitStart != -1)
            this.committingTime += (System.currentTimeMillis() - previousCommitStart);
    }

    public void finishWaitingForOthers() {
        if (previousWaitingForOthersStart != -1)
            this.waitingForOthersTime += (System.currentTimeMillis() - previousWaitingForOthersStart);
    }

    public void appendWAL(byte[] initValue) {
        this.WAL.add(initValue);
    }
}
