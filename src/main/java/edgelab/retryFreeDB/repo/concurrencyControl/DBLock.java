package edgelab.retryFreeDB.repo.concurrencyControl;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;

@Slf4j
class DBLock {
    @Override
    public String toString() {
        return "Lock<" + resource + ", holding by: " + holdingTransactions + ", pending: " + pendingTransactions + ", next in line "+pendingTransactions.peek()+">";
    }

    private final String resource;
    private final Set<DBTransaction> holdingTransactions;
//    private final Map<String, LockType> transactionLockTypes;
    private LockType ownersLockType;
    private final Queue<DBTransaction> retiredTransactions;
    private final Map<DBTransaction, LockType> retiredLockTypes;
    @Getter
    private final Queue<DBTransaction> pendingTransactions;
    private final Map<DBTransaction, LockType> pendingLockTypes;


    public static boolean BAMBOO_ENABLE = false;
    public static boolean WOUND_WAIT_ENABLE = false;


    public DBLock(String resource, Boolean atomicLock) {
        this.resource = resource;
        this.holdingTransactions = new HashSet<>();
        this.retiredTransactions =  new PriorityQueue<>(new Comparator<DBTransaction>() {
            @Override
            public int compare(DBTransaction s1, DBTransaction s2) {
                return Long.compare(Long.parseLong(s1.getTimestamp()) ,Long.parseLong(s2.getTimestamp()));
            }
        });
        this.retiredLockTypes = new HashMap<>();
        this.ownersLockType = null;
        this.pendingLockTypes = new HashMap<>();

        if (!atomicLock)
            this.pendingTransactions = new PriorityQueue<>(new Comparator<DBTransaction>() {
                @Override
                public int compare(DBTransaction s1, DBTransaction s2) {
                    return Long.compare(Long.parseLong(s1.getTimestamp()) ,Long.parseLong(s2.getTimestamp()));

                }
            });
        else
            this.pendingTransactions = new PriorityQueue<>(new Comparator<DBTransaction>() {
                @Override
                public int compare(DBTransaction s1, DBTransaction s2) {
                    return Long.compare(Long.parseLong(s2.getTimestamp()) ,Long.parseLong(s1.getTimestamp()));

                }
            });
    }

    public synchronized boolean canGrant(DBTransaction transaction, LockType lockType) {
        if (isHeldBefore(transaction, lockType))
            return true;


        if (lockType == LockType.WRITE) {
//            either no lock held or the same tx has a read lock (upgrade r->w)
            if (holdingTransactions.isEmpty() || (holdingTransactions.size() == 1 && holdingTransactions.contains(transaction)))
                    if (pendingTransactions.contains(transaction) && pendingTransactions.peek().equals(transaction))
                        return true;
        }
        else {
            // Can grant read lock if the same transaction has a write lock on it (upgrade w->r)
            if (ownersLockType == LockType.WRITE && (!holdingTransactions.isEmpty() && holdingTransactions.contains(transaction)))
                return true;
            // Can grant read lock if no write lock is held
            if (ownersLockType == LockType.WRITE)
                return false;
            // Can grant read lock if no pending write lock is ahead
            for (DBTransaction value : pendingTransactions) {
                if (value.equals(transaction)) {
                    return true; // Reached the target value without finding any write values
                }
                if (pendingLockTypes.get(value).equals(LockType.WRITE)) {
                    return false;
                }
            }
            return true;
        }



        return false;
    }


    public synchronized void promoteWaiters() {
        while (!pendingTransactions.isEmpty()) {
            DBTransaction t = pendingTransactions.peek();
            LockType lockType = pendingLockTypes.get(t);

//            if (!isHeldBefore(t, pendingLockTypes.get(t))) // if pending is already taken
//                if (!holdingTransactions.isEmpty()) // no one holding
//                    if (!isUpgradeCase(t)) // same tx has the read lock
//                        if (conflict(pendingLockTypes.get(t), ownersLockType))
//                            break;
//
            boolean upgrade = false;
            if (canGrant(t, pendingLockTypes.get(t))) {
                if (isGoingToBeUpgraded(t, pendingLockTypes.get(t)))
                    upgrade = true;
                log.info("promoting transaction {}", t.toString());
                grant(t, pendingLockTypes.get(t));
            }
            else
                break;


            if (BAMBOO_ENABLE) {
                for (DBTransaction tp : retiredTransactions) {
                    if (!upgrade) // do not increase commit semaphore twice for an upgrade lock
                        if (conflict(retiredLockTypes.get(tp), lockType)) {

                            log.info("commit semaphore increased for transaction {}", t);
                            t.incCommitSemaphore();
                            break;
                        }
                }
            }
        }
    }
    private boolean isGoingToBeUpgraded(DBTransaction transaction, LockType lockType) {
        if (lockType == LockType.WRITE)
            return (holdingTransactions.size() == 1 && holdingTransactions.contains(transaction));
        else
            return (!holdingTransactions.isEmpty() && holdingTransactions.contains(transaction));

    }

    public synchronized boolean conflict(DBTransaction holdingTransaction, DBTransaction transaction, LockType lockType) {
        if (holdingTransaction.equals(transaction))
            return false;
        if (!holdingTransactions.contains(holdingTransaction) && retiredTransactions.contains(holdingTransaction))
            return lockType == LockType.WRITE || retiredLockTypes.get(holdingTransaction) == LockType.WRITE;
        return lockType == LockType.WRITE || ownersLockType == LockType.WRITE;
    }


    public synchronized boolean conflict(LockType lockType, LockType lockType2) {
        return lockType == LockType.WRITE || lockType2 == LockType.WRITE;
    }



    public synchronized void grant(DBTransaction transaction, LockType lockType) {
        pendingTransactions.remove(transaction);
        pendingLockTypes.remove(transaction);
        holdingTransactions.add(transaction);
        ownersLockType = lockType;
    }

    public synchronized void release(DBTransaction transaction) {
        holdingTransactions.remove(transaction);
        if (holdingTransactions.isEmpty())
            ownersLockType = null;


        pendingTransactions.remove(transaction);
        pendingLockTypes.remove(transaction);


        retiredTransactions.remove(transaction);
        retiredLockTypes.remove(transaction);
    }
    public void retire(DBTransaction transaction) {
//         TODO
        holdingTransactions.remove(transaction);
        retiredTransactions.add(transaction);
        retiredLockTypes.put(transaction, ownersLockType);
        if (holdingTransactions.isEmpty())
            ownersLockType = null;


    }

    public String getResource() {
        return resource;
    }

    public synchronized Set<DBTransaction> getHoldingTransactions() {
        return new HashSet<>(holdingTransactions);
    }

    public synchronized Set<DBTransaction> getRetiredTransactions() {
        return new HashSet<>(retiredTransactions);
    }


    public synchronized void addPending(DBTransaction transaction, LockType lockType) {
        if (!pendingTransactions.contains(transaction)) {
            pendingTransactions.offer(transaction);
            pendingLockTypes.put(transaction, lockType);
        }
        else {
            if (pendingLockTypes.get(transaction).equals(LockType.READ) && lockType.equals(LockType.WRITE))
                pendingLockTypes.put(transaction, lockType); //Upgrade the pending lock
        }
    }


    public boolean isHeldBefore(DBTransaction transaction, LockType lockType) {
        return holdingTransactions.contains(transaction) && ownersLockType == lockType;
    }

    public String printPendingLocks() {
        StringBuilder s = new StringBuilder();
        s.append("[ ");
        for (DBTransaction p : pendingTransactions) {
            s.append(p).append(":").append(pendingLockTypes.get(p));
            s.append(", ");
        }
        s.append("]");
        return s.toString();
    }

    public LockType getAllOwnerType(DBTransaction tx) {
        if (holdingTransactions.contains(tx))
            return ownersLockType;
        return retiredLockTypes.get(tx);
    }

    public boolean isHeadOfRetried(DBTransaction tx) {
        if (retiredTransactions.isEmpty())
            return false;
        return retiredTransactions.peek().equals(tx);
    }

    public boolean conflictWithRetriedHead(LockType txTupleType) {
        if (retiredTransactions.isEmpty())
            return false;
        return conflict(txTupleType, retiredLockTypes.get(retiredTransactions.peek()));
    }

    public DBTransaction getRetiredHead() {
        return retiredTransactions.peek();
    }

    public boolean isRetiredEmpty() {
        return retiredTransactions.isEmpty();
    }

    public void releasePending(DBTransaction transaction) {
        pendingTransactions.remove(transaction);
        if (holdingTransactions.contains(transaction))
            pendingLockTypes.put(transaction, LockType.READ);
        else
            pendingLockTypes.remove(transaction);
    }
}

enum LockType {
    READ,
    WRITE
}
