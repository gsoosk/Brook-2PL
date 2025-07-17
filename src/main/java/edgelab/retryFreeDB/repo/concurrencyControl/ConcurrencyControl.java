package edgelab.retryFreeDB.repo.concurrencyControl;

import com.google.common.base.Joiner;
import edgelab.proto.LockData;
import edgelab.retryFreeDB.benchmark.Performance;
import edgelab.retryFreeDB.repo.storage.DTO.DBData;
import edgelab.retryFreeDB.repo.storage.DTO.DBDeleteData;
import edgelab.retryFreeDB.repo.storage.DTO.DBInsertData;
import edgelab.retryFreeDB.repo.storage.DTO.DBWriteData;
import edgelab.retryFreeDB.repo.concurrencyControl.util.BiKeyHashMap;
import edgelab.retryFreeDB.repo.storage.InMemoryRepo;
import edgelab.retryFreeDB.repo.storage.KeyValueRepository;
import edgelab.retryFreeDB.repo.storage.PostgresRepo;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.units.qual.A;
import org.jgrapht.alg.util.Pair;
import org.rocksdb.RocksDBException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

@Slf4j
public class ConcurrencyControl {
    private static final String DEADLOCK_ERROR = "40P01";
    private static boolean SLW_ENABLE = false;
    private static boolean WOUND_WAIT_ENABLE = true;
    private static boolean BAMBOO_ENABLE = true;
    private static final boolean SHARED_LOCK_ENABLE = true;

    private static boolean OCC_ENABLE = false;
    private static boolean IC3_ENABLE = false;

    public static void setMode(String mode) throws Exception {
        switch (mode) {
            case "slw", "ordered" -> {
                ConcurrencyControl.BAMBOO_ENABLE = false;
                ConcurrencyControl.WOUND_WAIT_ENABLE = false;
                ConcurrencyControl.SLW_ENABLE = true;
                ConcurrencyControl.OCC_ENABLE = false;
                DBLock.BAMBOO_ENABLE = false;
                DBLock.WOUND_WAIT_ENABLE = false;
                DBLock.SLW_ENABLE = true;
            }
            case "ww" -> {
                ConcurrencyControl.BAMBOO_ENABLE = false;
                ConcurrencyControl.WOUND_WAIT_ENABLE = true;
                ConcurrencyControl.SLW_ENABLE = false;
                ConcurrencyControl.OCC_ENABLE = false;
                DBLock.BAMBOO_ENABLE = false;
                DBLock.WOUND_WAIT_ENABLE = true;
                DBLock.SLW_ENABLE = false;
            }
            case "bamboo" -> {
                ConcurrencyControl.BAMBOO_ENABLE = true;
                ConcurrencyControl.WOUND_WAIT_ENABLE = true;
                ConcurrencyControl.SLW_ENABLE = false;
                ConcurrencyControl.OCC_ENABLE = false;
                DBLock.BAMBOO_ENABLE = true;
                DBLock.WOUND_WAIT_ENABLE = true;
                DBLock.SLW_ENABLE = false;
            }
            case "occ" -> {
                ConcurrencyControl.BAMBOO_ENABLE = false;
                ConcurrencyControl.WOUND_WAIT_ENABLE = false;
                ConcurrencyControl.SLW_ENABLE = false;
                ConcurrencyControl.OCC_ENABLE = true;
                DBLock.BAMBOO_ENABLE = false;
                DBLock.WOUND_WAIT_ENABLE = false;
                DBLock.SLW_ENABLE = false;
            }
            case "ic3" -> {
                ConcurrencyControl.BAMBOO_ENABLE = false;
                ConcurrencyControl.WOUND_WAIT_ENABLE = false;
                ConcurrencyControl.SLW_ENABLE = false;
                ConcurrencyControl.OCC_ENABLE = false;
                ConcurrencyControl.IC3_ENABLE = true;
                DBLock.BAMBOO_ENABLE = false;
                DBLock.WOUND_WAIT_ENABLE = false;
                DBLock.SLW_ENABLE = false;
            }
            default -> throw new Exception("This mode of 2pl is not supported by server");
        }
    }




    private static final long LOCK_THINKING_TIME = 0;
    public static  long OPERATION_THINKING_TIME = 0;

    private String partitionId;

    private final BiKeyHashMap<String, String, String> dirtyReads = new BiKeyHashMap<>(); // <tx, resource, value>



    private KeyValueRepository keyValueRepository;


    private final Map<String, Map<String, String>> IC3StashDB = new ConcurrentHashMap<>();
    private final Map<String, List< Pair<DBTransaction, String>> > IC3LastAccess = new ConcurrentHashMap<>(); // k: [(T, "reader"/"writer),...]

    public ConcurrencyControl(String addr, String port) throws RocksDBException {
        String url = "jdbc:postgresql://" + addr + ":" + port + "/postgres";
//        TODO: fix rollback
        if (Objects.equals(port, "-1")) {
            keyValueRepository = new InMemoryRepo(addr);
            this.locks = new ConcurrentHashMap<>(((InMemoryRepo) keyValueRepository).getDBSize(), 0.75f);
        }
        else
            keyValueRepository = new PostgresRepo(url);

    }



    public void initTransaction(DBTransaction transaction) throws SQLException {
        if (keyValueRepository instanceof PostgresRepo) {
            Connection conn = ((PostgresRepo) keyValueRepository).connect();
            conn.setAutoCommit(false);
            transaction.setConnection(conn);
        }
    }






    private Map<String, DBLock> locks = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Set<String>> transactionResources = new ConcurrentHashMap<>();

    public void lock(DBTransaction tx, Set<DBTransaction> toBeAborted, LockData data) throws Exception {
        if (OCC_ENABLE || IC3_ENABLE)
            return;
        String resource = data.getResource();
        LockType lockType = LockType.WRITE;
        if (SHARED_LOCK_ENABLE)
            lockType = data.getLockType() == edgelab.proto.LockType.READ ? LockType.READ : LockType.WRITE;
        lockLogic(tx, toBeAborted, resource, lockType);
    }
    public void lock(DBTransaction tx, Set<DBTransaction> toBeAborted, DBData data) throws Exception {
        if (OCC_ENABLE)
            return;
        String resource = getResource(data);
        LockType lockType = LockType.WRITE;
        if (SHARED_LOCK_ENABLE)
            lockType = (!(data instanceof DBWriteData) && !(data instanceof DBInsertData) && !(data instanceof DBDeleteData)) ? LockType.READ : LockType.WRITE;
        lockLogic(tx, toBeAborted, resource, lockType);
    }

    private void lockLogic(DBTransaction tx, Set<DBTransaction> toBeAborted, String resource, LockType lockType) throws Exception {
        if (OCC_ENABLE)
            return;
        DBLock lock;
        log.info("{}, try to lock {}, <{}>", tx, resource, lockType);
        lock = locks.computeIfAbsent(resource, s -> new DBLock(s, false));


        synchronized (lock) {
            if (tx.isAbort()) {
                log.error("Transaction is aborted. Could not lock");
                throw new Exception("Transaction aborted. can not lock");
            }

            if (!lock.isHeldBefore(tx, lockType)) {

                handleConflict(tx, lockType, lock, toBeAborted);
                if(tx.isAbort()) {
                    log.error("Transaction is aborted. Could not lock");
                    throw new Exception("Transaction aborted. can not lock");
                }


                lock.addPending(tx, lockType);
                log.info("new pending transactons: {}", lock.printPendingLocks());
                tx.addResource(lock.getResource());


                lock.promoteWaiters();
                lock.notifyAll();

                while (!lock.isHeldBefore(tx, lockType)) {
                    try {
                        log.info("{}: waiting for lock on {}", tx, resource);
                        log.info("{}: previous holding transactons: {}",tx, lock.getHoldingTransactions());
                        tx.startWaiting();
                        lock.wait();
                        if (tx.isAbort()) {
                            log.error("Transaction is aborted. Could not lock");
                            throw new Exception("Transaction aborted. can not lock");
                        }
                        log.info("{}: wakes up to check the lock {}", tx, resource);
                        tx.wakesUp();

                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
            tx.addResource(lock.getResource());
            log.info("{}: Lock granted on {} for {}", tx, resource, lockType);
        }

        delay(LOCK_THINKING_TIME);
    }

    public void tryLock(DBTransaction tx, DBData data) throws Exception {
        if (OCC_ENABLE)
            return;

        if (BAMBOO_ENABLE || WOUND_WAIT_ENABLE)
            throw new Exception("Brook2PL is not enabled!");

        String resource = getResource(data);
        DBLock lock;
        LockType lockType = LockType.WRITE;
        if (SHARED_LOCK_ENABLE)
            lockType = (!(data instanceof DBWriteData) && !(data instanceof DBInsertData) && !(data instanceof DBDeleteData)) ? LockType.READ : LockType.WRITE;



        log.info("{}, try to lock {}, <{}>",tx, resource, lockType);
        lock = locks.computeIfAbsent(resource, s -> new DBLock(s, false));


        synchronized (lock) {
            if (tx.isAbort()) {
                log.error("Transaction is aborted. Could not lock");
                throw new Exception("Transaction aborted. can not lock");
            }

            if (!lock.isHeldBefore(tx, lockType)) {

                lock.addPending(tx, lockType);
                log.info("new pending transactons: {}", lock.printPendingLocks());


                lock.promoteWaiters();
                lock.notifyAll();

                if (!lock.isHeldBefore(tx, lockType)) {
                    lock.releasePending(tx);
                    throw new Exception("Try failed: tx is not granted!");
                }
            }
            tx.addResource(lock.getResource());
            log.info("{}: Lock granted on {} for {}", tx, resource, lockType);
        }

        delay(LOCK_THINKING_TIME);
    }

    public boolean waitDieLock(DBTransaction tx, String resource) throws Exception {
        if (OCC_ENABLE)
            return true;

        // this method orders transactions based on
        if (BAMBOO_ENABLE || WOUND_WAIT_ENABLE)
            throw new Exception("Brook2PL is not enabled!");

        DBLock lock;
        LockType lockType = LockType.WRITE;


        log.info("{}, try to lock {}, <{}>",tx, resource, lockType);
        lock = locks.computeIfAbsent(resource, s -> new DBLock(s, true));


        synchronized (lock) {
            if (tx.isAbort()) {
                log.error("Transaction is aborted. Could not lock");
                throw new Exception("Transaction aborted. can not lock");
            }

            if (!lock.isHeldBefore(tx, lockType)) {



                boolean hasConflict = false;
                log.info("previous holding transactons: {}", lock.getHoldingTransactions());
                log.info("previous pending transactons: {}", lock.printPendingLocks());
                log.info("previous retired transactons: {}", lock.getRetiredTransactions());
                Set<DBTransaction> toCheckTransactions = new HashSet<>(lock.getHoldingTransactions());
//                toCheckTransactions.add(lock.getPendingTransactions().getLast());


                for (DBTransaction t : toCheckTransactions) {
                    if (lock.conflict(t, tx, lockType)) {
                        hasConflict = true;
                        log.info("{}: conflict detected {}", tx, t);
                    }

                    // can not lock if tx is younger
                    if (hasConflict && Long.parseLong(t.toString()) < Long.parseLong(tx.toString())) {
                        log.info("{}: can not lock {} -- too young", tx, resource);
                        return false;
                    }
                }




                lock.addPending(tx, lockType);
                log.info("new pending transactons: {}", lock.printPendingLocks());


                lock.promoteWaiters();
                lock.notifyAll();

                while (!lock.isHeldBefore(tx, lockType)) {
                    try {
                        log.info("{}: waiting for lock on {}", tx, resource);
                        tx.startWaiting();
                        lock.wait();
                        if (tx.isAbort()) {
                            log.error("Transaction is aborted. Could not lock");
                            throw new Exception("Transaction aborted. can not lock");
                        }
                        log.info("{}: wakes up to check the lock {}", tx, resource);
                        tx.wakesUp();

                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
            tx.addResource(lock.getResource());
            log.info("{}: Lock granted on {} for {}", tx, resource, lockType);
        }

        delay(LOCK_THINKING_TIME);
        return true;
    }
    
    public void atomicLock(DBTransaction tx, String tableName, List<String> values, Set<DBTransaction> toBeAborted) throws Exception {
        if (OCC_ENABLE)
            return;
        boolean allLocked = false;
        while (!allLocked) {
            for (int i = 0; i < values.size(); i++) {

                String resource = getResource(tableName, values.get(i));
                if (!waitDieLock(tx, resource)) {
                    for (int j = 0 ; j < i; j++) {
                        String releaseResource = getResource(tableName, values.get(j));
                        releaseLock(tx, releaseResource, toBeAborted);
                    }
                    tx.startWaiting();
                    // Retry to get the locks in order after 8 micro second
                    Thread.sleep(0, 8 * 1000);
                    tx.wakesUp();
                    break;
                }
                if (i == values.size() - 1)
                    allLocked = true;
            }
        }
    }

    private static String getResource(String tableName, String value) {
        return tableName + "," + value;
    }


    private static String getResource(DBData data) {
        return (data instanceof DBInsertData) ? getResource(data.getTable(), ((DBInsertData) data).getRecordId()) : getResource(data.getTable(), Joiner.on(",").join(data.getQueries()));
    }

    private void addNewResourceForTransaction(String tx, String resource) {
        transactionResources.putIfAbsent(tx, new HashSet<>());
        transactionResources.get(tx).add(resource);
    }

    private void handleConflict(DBTransaction tx, LockType lockType, DBLock lock, Set<DBTransaction> toBeAborted) {
        if (SLW_ENABLE && !Performance.INTERACTIVE) // no need to handle conflict in slw stored procedure mode
            return;

        synchronized (lock) {
                boolean hasConflict = false;
                log.info("previous holding transactons: {}", lock.getHoldingTransactions());
                log.info("previous pending transactons: {}", lock.printPendingLocks());
                log.info("previous retired transactons: {}", lock.getRetiredTransactions());

                Set<DBTransaction> toCheckTransactions = new HashSet<>(lock.getHoldingTransactions());
                if (BAMBOO_ENABLE)
                        toCheckTransactions.addAll(lock.getRetiredTransactions());
                for (DBTransaction t : toCheckTransactions) {
                    if (lock.conflict(t, tx, lockType)) {
                        hasConflict = true;
                        log.info("{}: conflict detected {}", tx, t);
                    }
                    if (!hasConflict)
                        continue;
                    if (WOUND_WAIT_ENABLE) {
                        if (Long.parseLong(t.toString()) > Long.parseLong(tx.toString())) {
//                    abort transaction t
                            toBeAborted.add(t);
                            t.setAbort();
                            releaseLockWithoutPromotion(t, lock, toBeAborted);


//                    unlockAll(t);
//                    lock.release(t);
//                    transactionResources.get(t).remove(lock.getResource());

                            log.info("{}: Wound-wait: {} aborted by {}", tx, t, tx);
                            log.info("after holding transactons: {}", lock.getHoldingTransactions());
//                        return;
                        }
//                        T_i = TX , T_j = T
                    } else if (SLW_ENABLE) {
//                        T_j->Dynamic
//                        t(T_i) < t(T_j) -> T_i older than T_j, T_j is younger than T_i -> abort T_i
                        if (t.isInteractiveTransaction() && Long.parseLong(t.toString()) > Long.parseLong(tx.toString())) { // ww for interactive transactions
                            toBeAborted.add(t);
                            t.setAbort();
                            releaseLockWithoutPromotion(t, lock, toBeAborted);
                        }
                        else if (t.isInteractiveTransaction() && tx.isInteractiveTransaction()) {
                            continue;
                        }
//                        T_i->Dynamic, T_j->Static
//                        t(T_i) < t(T_j) -> T_i older than T_j -> abort T_i or T_j is younger than T_i
                        else if (tx.isInteractiveTransaction() && Long.parseLong(t.toString()) > Long.parseLong(tx.toString())) {
                            toBeAborted.add(tx);
                            tx.setAbort();
                            releaseLockWithoutPromotion(tx, lock, toBeAborted);
                        }
                    }
                }
        }
    }

//
//
//    public void lock(String tx, Connection conn, Set<String> toBeAborted, DBData data) throws SQLException {
//
//        log.warn("{}, Acquiring lock for data, {}:<{},{}>",tx, data.getTable(), data.getId(), data.getQuery());
//
//        String s = (data instanceof DBInsertData) ? data.getTable() +  ","  + ((DBInsertData) data).getRecordId() : data.getTable() +  ","  + data.getQuery();
//        getLock(s + "+1").lock();
////      Assumption: Only lock based on the primary key
//
//        getLock(s + "+1").unlock();
//        getLock(s + "+2").lock();
//        if (!(data instanceof DBInsertData)) {
////        FIXME: Risk of sql injection
////            String lockSQL = "SELECT * FROM " + data.getTable() + " WHERE " + data.getId() + " = ? FOR UPDATE";
////            try (PreparedStatement updateStmt = conn.prepareStatement(lockSQL))
//            try
//            {
////                updateStmt.setInt(1, data.getQuery());
////                ResultSet rs = updateStmt.executeQuery();
////                if (!rs.next()) {
////                    log.info("no row with id found!");
//                getAdvisoryLock(conn, data.getTable(), data.getQuery());
////                }
//
//                delay(LOCK_THINKING_TIME);
//            } catch (SQLException ex) {
//
//                log.error("{}, db error: couldn't lock,  {}:CODE:{}", tx, ex.getMessage(), ex.getSQLState());
//                throw ex;
//            }
//            log.warn("{}, Locks on rows acquired, {}:<{},{}>", tx, data.getTable(), data.getId(), data.getQuery());
//        }
//        else {
//            getAdvisoryLock(conn, data.getTable(), Integer.parseInt (((DBInsertData) data).getRecordId()));
//        }
//
//        getLock(s + "+2").unlock();
//    }



    private void delay(long duration) {
        try {
            Thread.sleep(duration);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static void getAdvisoryLock(Connection conn, String tableName, Integer id) throws SQLException {
        String lockSQL = "SELECT pg_advisory_lock('" + tableName + "'::regclass::integer, ?)";
        try (PreparedStatement updateStmt = conn.prepareStatement(lockSQL)) {
            updateStmt.setInt(1, id);
            updateStmt.executeQuery();
        } catch (SQLException ex) {
            log.info("db error: couldn't lock,  {}", ex.getMessage());
            throw ex;
        }
        log.info("Advisory lock on {},{} is acquired", tableName, id);
    }

    private  void unlockAllAdvisoryLocks(String tx, Connection conn) throws SQLException {
        String lockSQL = "SELECT pg_advisory_unlock_all()";
        try (PreparedStatement updateStmt = conn.prepareStatement(lockSQL)) {
            updateStmt.executeQuery();
        } catch (SQLException ex) {
            log.info("db error: couldn't unlock all,  {}", ex.getMessage());
            throw ex;
        }
        log.info("All Advisory locks unlocked");

    }


    public void unlock(DBTransaction tx, DBData data, Set<DBTransaction> toBeAborted) throws Exception {
        String resource = getResource(data);

        if (keyValueRepository instanceof PostgresRepo) {
//            String dirtyRead = keyValueRepository.read(tx, data);
//            synchronized (dirtyReads) {
//                log.info("{}: adding dirty read for {}", tx, resource);
//                dirtyReads.put(tx.toString(), resource, dirtyRead);
//            }
        }

        releaseLock(tx, resource, toBeAborted);
    }

    private void releaseLock(DBTransaction tx, String resource, Set<DBTransaction> toBeAborted) {
        DBLock lock = locks.get(resource);

        releaseLockWithoutPromotion(tx, lock, toBeAborted);
        synchronized (lock) {
            lock.promoteWaiters();
            lock.notifyAll(); // Notify all waiting threads
//            if (lock.isEmpty())
//                locks.remove(resource);
        }
    }

    private void releaseLockWithoutPromotion(DBTransaction tx, DBLock lock, Set<DBTransaction> toBeAborted) {
        if (lock != null) {
            synchronized (lock) {
                if(BAMBOO_ENABLE) {

                    HashSet<DBTransaction> allOwners = new HashSet<>(lock.getHoldingTransactions());
                    allOwners.addAll(lock.getRetiredTransactions());

                    if (!allOwners.contains(tx)) { // Just pending
                        lock.release(tx);
                        tx.removeResource(lock.getResource());
                    }
                    else {
                        LockType txTupleType = lock.getAllOwnerType(tx);
//                    Cascading aborts
                        if (tx.isAbort() && txTupleType.equals(LockType.WRITE)) {
//                        abort all transactions in all_owners after txn
                            for (DBTransaction t : allOwners) {
                                if (t.getTimestampValue() > tx.getTimestampValue()) {
                                    toBeAborted.add(t);
                                    t.setAbort();
                                    log.info("{}: cascading abort: {}", tx, t);
                                }
                            }
                        }

                        boolean wasHead = lock.isHeadOfRetried(tx);


                        lock.release(tx);
                        tx.removeResource(lock.getResource());

                        if (wasHead) {
                            if (lock.isRetiredEmpty()) {
                                for (DBTransaction t : lock.getHoldingTransactions()) {
                                    t.decCommitSemaphore();
                                    log.info("commit semaphore decreased for transaction {}", t);
                                }
                            } else if (lock.conflictWithRetriedHead(txTupleType)) {
                                lock.getRetiredHead().decCommitSemaphore();
                                log.info("commit semaphore decreased for transaction {}", lock.getRetiredHead());
                            }
                        }
                    }

                }
                else {
                    lock.release(tx);
                    tx.removeResource(lock.getResource());

                }


                log.info("{}: Lock released  on {}", tx, lock.getResource());

            }
        }
        else {
            log.error("{}, Lock was not held to be released!", tx);
        }
    }

    public void unlockAll(DBTransaction tx, Set<DBTransaction> toBeAborted) {
        if (OCC_ENABLE | IC3_ENABLE)
            return;
//        synchronized (locks) {
        for (String resource : tx.getResources()) {
                releaseLock(tx, resource, toBeAborted);
            }
            tx.clearResources();

            if (keyValueRepository instanceof PostgresRepo) {
                synchronized (dirtyReads) {
                    log.info("{}: removing dirty reads for tx", tx);
                    dirtyReads.removeByTransaction(tx.toString());
                }
            }
//        }


    }

    public void retireLock(DBTransaction tx, DBData data) throws Exception {
        if (!BAMBOO_ENABLE)
            throw new Exception("Bamboo is not enabled!");


        String resource = getResource(data);
        DBLock lock;

        log.info("{}, retiring the lock {}", tx, resource);
        lock = locks.computeIfAbsent(resource, s -> new DBLock(s, false));

        if (lock != null) {
            synchronized (lock) {
                if (!lock.getHoldingTransactions().contains(tx))
                    throw new Exception("Bamboo: does not hold the lock to retire");


                lock.retire(tx);

                log.info("{}: Lock retired  on {}", tx, lock.getResource());

                lock.promoteWaiters();
                lock.notifyAll(); // Notify all waiting threads

            }

            if (keyValueRepository instanceof PostgresRepo) {
//                String dirtyRead = keyValueRepository.read(tx, data);
//                synchronized (dirtyReads) {
//                    log.info("{}: adding dirty read for {}", tx, resource);
//                    dirtyReads.put(tx.toString(), resource, dirtyRead);
//                }
            }
        }
        else {
            log.error("{}, Lock was not held to be retired!", tx);
        }



    }

//
//    public void unlock(String tx, ConcurrentHashMap<String, Connection> transactions, DBData data) throws SQLException {
//        bambooReleaseLock(tx, data.getTable() + "," + data.getQuery());
//        unlockAdvisory(transactions.get(tx), data.getTable(), data.getQuery());
//    }


//
//    private static void unlockAdvisory(Connection conn, String tableName, Integer id) throws SQLException {
//        String lockSQL = "SELECT pg_advisory_unlock('" + tableName + "'::regclass::integer, ?)";
//        try (PreparedStatement updateStmt = conn.prepareStatement(lockSQL)) {
//            updateStmt.setInt(1, id);
//            updateStmt.executeQuery();
//        } catch (SQLException ex) {
//            log.error("db error: couldn't unlock,  {}", ex.getMessage());
//            throw ex;
//        }
//        log.info("Advisory lock unlocked {},{}", tableName, id);
//    }

//
//    public void lockTable(Connection conn, DBInsertData data) throws SQLException {
//        log.info("Acquiring table lock for data");
////        FIXME: Risk of sql injection
//        String lockSQL = "LOCK TABLE "+ data.getTable() +" IN ACCESS EXCLUSIVE";
//        try (PreparedStatement updateStmt = conn.prepareStatement(lockSQL)) {
//            updateStmt.executeQuery();
//        }
//        catch (SQLException ex) {
//            log.info("db error: couldn't lock the table,  {}", ex.getMessage());
//            throw ex;
//        }
//        log.info("Locks on table {} acquired", data.getTable());
//    }
    public void release(DBTransaction tx, Set<DBTransaction> toBeAborted) throws Exception {
        try {
            keyValueRepository.commit(tx);
//            unlockAllAdvisoryLocks(tx, conn);
            unlockAll(tx, toBeAborted);

        } catch (Exception e) {
            log.error("Could not release the locks: {}", e.getMessage());
            throw e;
        }
    }


    public void rollback(DBTransaction tx,  Set<DBTransaction> toBeAborted) throws Exception {
        try {
            keyValueRepository.rollback(tx);
            unlockAll(tx, toBeAborted);
        } catch (Exception e) {
            log.error("Could not rollback and release the locks: {}", e.getMessage());
            throw e;
        }

    }





    public Map<String, String> get(DBTransaction tx, DBData data) throws Exception {
        String resource = getResource(data);


        long last = System.currentTimeMillis();
        if (keyValueRepository instanceof PostgresRepo) {
//            synchronized (dirtyReads) {
//                if (dirtyReads.containsResource(resource)) {
//                    log.info("{}: dirty read of {}", tx, resource);
//                    delay(OPERATION_THINKING_TIME);
//                    return dirtyReads.getByResource(resource);
//                }
//            }

            log.info("{}: dirty read time: {}", tx.getTimestamp(), System.currentTimeMillis() - last);
        }


        Map<String, String> value;

        String key = data.getKey();
        if (OCC_ENABLE && tx.getOCCWriteSet().containsKey(key)) {
            if (tx.getOCCWriteSet().get(key) instanceof DBInsertData)
                value = InMemoryRepo.getInsertDataMap((DBInsertData) tx.getOCCWriteSet().get(key));
            else if (tx.getOCCWriteSet().get(key) instanceof DBDeleteData)
                throw new RocksDBException("does not exits");
            else {
                value = keyValueRepository.read(tx, data);
                InMemoryRepo.updateValue((DBWriteData) tx.getOCCWriteSet().get(key), value);
            }
        }
        else if (IC3_ENABLE && tx.getActiveIC3Piece().getWriteSet().containsKey(key)) { // Read from local piece
            if (tx.getOCCWriteSet().get(key) instanceof DBInsertData)
                value = InMemoryRepo.getInsertDataMap((DBInsertData) tx.getActiveIC3Piece().getWriteSet().get(key));
            else if (tx.getOCCWriteSet().get(key) instanceof DBDeleteData)
                throw new RocksDBException("does not exits");
            else {
                if (IC3StashDB.containsKey(key))
                    value = IC3StashDB.get(key);
                else
                    value = keyValueRepository.read(tx, data);

                if (value.isEmpty())
                    throw new RocksDBException("does not exits");
                InMemoryRepo.updateValue((DBWriteData) tx.getActiveIC3Piece().getWriteSet().get(key), value);
            }
        }
        else if (IC3_ENABLE && IC3StashDB.containsKey(key)) { // Read from stash
            log.info("{}: IC3: Read from stashed data", tx.getTimestamp());
            value = IC3StashDB.get(key);
            if (value.isEmpty())
                throw new RocksDBException("does not exits");
        }
        else
            value = keyValueRepository.read(tx, data);

        if (OCC_ENABLE)
            tx.addOCCRead(key);
        else if (IC3_ENABLE)
            tx.getActiveIC3Piece().addRead(key);



        delay(OPERATION_THINKING_TIME);
        return value;
    }


    public void insert(DBTransaction tx, DBInsertData d) throws Exception {
        if (OCC_ENABLE)
            tx.addOCCWrite(InMemoryRepo.getInsertKey(d), d);
        else if (IC3_ENABLE) {
            tx.addOCCWrite(InMemoryRepo.getInsertKey(d), d);
            tx.getActiveIC3Piece().addWrite(InMemoryRepo.getInsertKey(d), d);
        }
        else
            keyValueRepository.insert(tx, d);
    }

    public void update(DBTransaction tx, DBWriteData d) throws Exception {

        if (OCC_ENABLE) {
            String key = d.getKey();
            tx.addOCCWrite(key, d);
        }
        else if (IC3_ENABLE) {
            tx.addOCCWrite(d.getKey(), d);
            tx.getActiveIC3Piece().addWrite(d.getKey(), d);
        }
        else
            keyValueRepository.write(tx, d);
    }

    public void remove(DBTransaction tx, DBDeleteData d) throws Exception {
        if (OCC_ENABLE) {
            String key = d.getKey();
            tx.addOCCWrite(key, d);
        }
        else if (IC3_ENABLE) {
            tx.addOCCWrite(d.getKey(), d);
            tx.getActiveIC3Piece().addWrite(d.getKey(), d);
        }
        else
            keyValueRepository.remove(tx, d);
    }

    public int lastId(String table) throws Exception {
        return keyValueRepository.lastId(table);
    }

    public void updateOCC(DBTransaction tx, DBData data) throws Exception {
        if (data instanceof DBInsertData)
            keyValueRepository.insert(tx, (DBInsertData) data);
        else if (data instanceof DBDeleteData)
            keyValueRepository.remove(tx, (DBDeleteData) data);
        else if (data instanceof DBWriteData)
            keyValueRepository.write(tx, (DBWriteData) data);
    }

    public void updateIC3Piece(DBTransaction tx, IC3Piece piece) throws Exception {
        for (String key : piece.getReadSet()) {
            this.IC3LastAccess.computeIfAbsent(key, k -> new ArrayList<>());

            if (!this.IC3LastAccess.get(key).isEmpty()) {
                DBTransaction tw = getLastWriteAccess(this.IC3LastAccess.get(key));
                if (tw != null)
                    tx.addIC3Dependency(tw);
            }
            this.IC3LastAccess.get(key).add(new Pair<>(tx, "reader"));
        }

        for (String key : piece.getWriteSet().keySet()) {
            this.IC3LastAccess.computeIfAbsent(key, k -> new ArrayList<>());

            if (!this.IC3LastAccess.get(key).isEmpty()) {
                DBTransaction trw = this.IC3LastAccess.get(key).get(this.IC3LastAccess.get(key).size() - 1).getFirst();
                tx.addIC3Dependency(trw);
            }

            this.IC3LastAccess.get(key).add(new Pair<>(tx, "writer"));
            updateIC3Stash(tx, piece, key);
        }
    }

    private DBTransaction getLastWriteAccess(List<Pair<DBTransaction, String>> pairs) {
        for (int i = pairs.size() - 1; i >= 0; i--) {
            if (pairs.get(i).getSecond().equals("writer")) {
                return pairs.get(i).getFirst();
            }
        }

        return null;
    }

    private void updateIC3Stash(DBTransaction tx, IC3Piece piece, String key) throws Exception {
        if (piece.getWriteSet().get(key) instanceof DBInsertData data) {
            log.info("{}: insert stash {}:{}", tx.getTimestamp(), data.getTable(), data.getRecordId());
            Map<String, String> value = InMemoryRepo.getInsertDataMap(data);
            if (!data.getTable().equals("order_line") && !data.getTable().equals("new_order") && !data.getTable().equals("history") && !data.getTable().equals("oorder")) { // do not save orderline and neworder due to memory limit
                IC3StashDB.put(key, value);
            }
        }
        else if (piece.getWriteSet().get(key) instanceof DBDeleteData) {
            IC3StashDB.put(key, new HashMap<>());
        }
        else if (piece.getWriteSet().get(key) instanceof DBWriteData data) {
            Map<String, String> value = keyValueRepository.read(tx, data);
            InMemoryRepo.updateValue(data, value);
            IC3StashDB.put(key, value);
        }
    }


    public void updateIC3(DBTransaction tx, DBData data, String key) throws Exception {
        updateOCC(tx, data);
        this.IC3LastAccess.computeIfAbsent(key, k -> new ArrayList<>());
        removeFromAccessList(tx, IC3LastAccess.get(key));
    }

    private static void removeFromAccessList(DBTransaction tx, List<Pair<DBTransaction, String>> pairs) {
        boolean removedReader = false;
        boolean removedWriter = false;

        for (int i = pairs.size() - 1; i >= 0; i--) {
            if (!removedWriter && pairs.get(i).getFirst().equals(tx) && pairs.get(i).getSecond().equals("writer")) {
                pairs.remove(i);
                removedWriter = true;
            }
            else if (!removedReader && pairs.get(i).getFirst().equals(tx) && pairs.get(i).getSecond().equals("reader")) {
                pairs.remove(i);
                removedReader = true;
            }

            if (removedWriter && removedReader)
                break;
        }
    }
}



