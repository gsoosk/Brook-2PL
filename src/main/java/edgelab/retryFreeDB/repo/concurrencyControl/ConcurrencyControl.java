package edgelab.retryFreeDB.repo.concurrencyControl;

import com.google.common.base.Joiner;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import edgelab.retryFreeDB.repo.storage.DTO.DBData;
import edgelab.retryFreeDB.repo.storage.DTO.DBDeleteData;
import edgelab.retryFreeDB.repo.storage.DTO.DBInsertData;
import edgelab.retryFreeDB.repo.storage.DTO.DBWriteData;
import edgelab.retryFreeDB.repo.concurrencyControl.util.BiKeyHashMap;
import edgelab.retryFreeDB.repo.storage.KeyValueRepository;
import edgelab.retryFreeDB.repo.storage.PostgresRepo;
import edgelab.retryFreeDB.repo.storage.RocksDBRepo;
import lombok.extern.slf4j.Slf4j;
import org.postgresql.PGConnection;
import org.rocksdb.RocksDBException;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class ConcurrencyControl {
    private static final String DEADLOCK_ERROR = "40P01";
    private static boolean WOUND_WAIT_ENABLE = true;
    private static boolean BAMBOO_ENABLE = true;
    private static final boolean SHARED_LOCK_ENABLE = true;

    //WALEED: is this function based on the algorithm right?
    public static void setMode(String mode) throws Exception {
        switch (mode) {
            case "slw", "ic3" -> {
                ConcurrencyControl.BAMBOO_ENABLE = false;
                ConcurrencyControl.WOUND_WAIT_ENABLE = false;
                DBLock.BAMBOO_ENABLE = false;
            }
            case "ww" -> {
                ConcurrencyControl.BAMBOO_ENABLE = false;
                ConcurrencyControl.WOUND_WAIT_ENABLE = true;
                DBLock.BAMBOO_ENABLE = false;
            }
            case "bamboo" -> {
                ConcurrencyControl.BAMBOO_ENABLE = true;
                ConcurrencyControl.WOUND_WAIT_ENABLE = true;
                DBLock.BAMBOO_ENABLE = true;
            }
            default -> throw new Exception("This mode of 2pl is not supported by server");
        }
    }

    private static final long LOCK_THINKING_TIME = 0;
    public static  long OPERATION_THINKING_TIME = 0;

    private String partitionId;

    private final BiKeyHashMap<String, String, String> dirtyReads = new BiKeyHashMap<>(); // <tx, resource, value>

    // WALEED: interface contain operations insert, remove, write, ...etc  (DBTransaction tx, DBInsertData data).
    private KeyValueRepository keyValueRepository;

    public ConcurrencyControl(String addr, String port) throws RocksDBException {
        String url = "jdbc:postgresql://" + addr + ":" + port + "/postgres";
//        TODO: fix rollback
        if (Objects.equals(port, "-1"))
            keyValueRepository = new RocksDBRepo(addr);// WALEED: this class connect to db and implements keyValueRepository interface
        else
            keyValueRepository = new PostgresRepo(url);

    }

    // WALEED: DBTransaction class contain transaction details ex: TS, lockingTime, resources, and so on.
    public void initTransaction(DBTransaction transaction) throws SQLException {
        if (keyValueRepository instanceof PostgresRepo) {
            Connection conn = ((PostgresRepo) keyValueRepository).connect(); // WALEED: why we need to set up connection again?
            conn.setAutoCommit(false);
            transaction.setConnection(conn);
        }
    }

    // WALEED: Locks {resourceX = Tx, resourceY = Ty, ...}? but why we have DBlock? if it contains all lock management?
    private final Map<String, DBLock> locks = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Set<String>> transactionResources = new ConcurrentHashMap<>();
    public void lock(DBTransaction tx, Set<DBTransaction> toBeAborted, DBData data) throws Exception {
        String resource = getResource(data);
        DBLock lock;
        LockType lockType = LockType.WRITE;
        if (SHARED_LOCK_ENABLE)//WALEED: As I see always true, right?
            lockType = (!(data instanceof DBWriteData) && !(data instanceof DBInsertData) && !(data instanceof DBDeleteData)) ? LockType.READ : LockType.WRITE;

        log.info("{}, try to lock {}, <{}>",tx, resource, lockType);
//        computeIfAbsent computes a value for a given key if it is not already present in the map.
//        If the key exists, returns the existing value. else it computes a new value using the provided function, puts it into the map
        lock = locks.computeIfAbsent(resource, DBLock::new);


        synchronized (lock) {
            if (tx.isAbort()) {
                log.error("Transaction is aborted. Could not lock");
                throw new Exception("Transaction aborted. can not lock");
            }
            // WALEED: I didn't get it if statement?
            if (!lock.isHeldBefore(tx, lockType)) {

                handleConflict(tx, lockType, lock, toBeAborted);


                lock.addPending(tx, lockType);
                log.info("new pending transactons: {}", lock.printPendingLocks());
                tx.addResource(lock.getResource());


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
    }

    private static String getResource(DBData data) {
        return (data instanceof DBInsertData) ? data.getTable() + "," + ((DBInsertData) data).getRecordId() : data.getTable() + "," + Joiner.on(",").join(data.getQueries());
    }

    private void addNewResourceForTransaction(String tx, String resource) {
        transactionResources.putIfAbsent(tx, new HashSet<>());
        transactionResources.get(tx).add(resource);
    }

    private void handleConflict(DBTransaction tx, LockType lockType, DBLock lock, Set<DBTransaction> toBeAborted) {
        synchronized (lock) {
            // why all logic inside WW mode?
            if (WOUND_WAIT_ENABLE) {
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

                    if (hasConflict && Long.parseLong(t.toString()) > Long.parseLong(tx.toString())) {
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
    // WALEED: Used for what? I didn't get it
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
    // WALEED: Used for what? I didn't get it
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


    // WALEED: after writing the new value we put this value in the dirty read map, note: we need to delete it after commit, right?
    public void unlock(DBTransaction tx, DBData data, Set<DBTransaction> toBeAborted) throws Exception {
        String resource = getResource(data);

        if (keyValueRepository instanceof PostgresRepo) {
            String dirtyRead = keyValueRepository.read(tx, data);
            synchronized (dirtyReads) {
                log.info("{}: adding dirty read for {}", tx, resource);
                dirtyReads.put(tx.toString(), resource, dirtyRead);
            }
        }

        releaseLock(tx, resource, toBeAborted);
    }

    // WALEED: after commit in 2pl and in brook?
    private void releaseLock(DBTransaction tx, String resource, Set<DBTransaction> toBeAborted) {
        DBLock lock = locks.get(resource);
        //System.out.println("Waleed tx"+tx);
        //System.out.println("XSDSSX"+getResource(new DBData()));
        //System.out.println("Waleed lock"+lock);
        //System.out.println("Waleed res"+resource);

        releaseLockWithoutPromotion(tx, lock, toBeAborted);
        synchronized (lock) {
            lock.promoteWaiters();
            lock.notifyAll(); // Notify all waiting threads
            //System.out.println("Waleed tx"+tx);
            //System.out.println("Waleed lock release"+lock);
            //System.out.println("Waleed res release"+resource);
        }
    }

    // WALEED: handling if there is anything to do after release lock like abort cascade
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

    // WALEED: unlockAll and free the resources and delete dirty values.
    public void unlockAll(DBTransaction tx, Set<DBTransaction> toBeAborted) {
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
        lock = locks.computeIfAbsent(resource, DBLock::new);

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
                String dirtyRead = keyValueRepository.read(tx, data);
                synchronized (dirtyReads) {
                    log.info("{}: adding dirty read for {}", tx, resource);
                    dirtyReads.put(tx.toString(), resource, dirtyRead);
                }
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

    //WALEED: release all or unlockAll all resources when TX is commited
    public void release(DBTransaction tx, Set<DBTransaction> toBeAborted) throws Exception {
        try {
            keyValueRepository.commit(tx);// But this method is empty? nothing there
//            System.out.println("waleed test "+tx.getResources()+ tx.getAbort());
//            unlockAllAdvisoryLocks(tx, conn);
            unlockAll(tx, toBeAborted);

        } catch (Exception e) {
            log.error("Could not release the locks: {}", e.getMessage());
            throw e;
        }
    }
    public boolean occValidate(DBTransaction tx) throws Exception{
        try {
            keyValueRepository.commit(tx);
            return true;
        } catch (Exception e) {
            log.error("Could not commit the transaction: {}", e.getMessage());
            return false;
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

    // WALEED: Get what information? A TX info?
    public String get(DBTransaction tx, DBData data) throws Exception {
        String resource = getResource(data);

        long last = System.currentTimeMillis();
        if (keyValueRepository instanceof PostgresRepo) {
            synchronized (dirtyReads) {
                if (dirtyReads.containsResource(resource)) {
                    log.info("{}: dirty read of {}", tx, resource);
                    delay(OPERATION_THINKING_TIME);
                    return dirtyReads.getByResource(resource);
                }
            }

            log.info("{}: dirty read time: {}", tx.getTimestamp(), System.currentTimeMillis() - last);
        }


        String value = keyValueRepository.read(tx, data);
        delay(OPERATION_THINKING_TIME);
        return value;
    }
    public String getStashedValue(DBTransaction tx, DBData data) throws Exception {
        String resource = getResource(data);

        long last = System.currentTimeMillis();
        if (keyValueRepository instanceof PostgresRepo) {
            synchronized (dirtyReads) {
                if (dirtyReads.containsResource(resource)) {
                    log.info("{}: dirty read of {}", tx, resource);
                    delay(OPERATION_THINKING_TIME);
                    return dirtyReads.getByResource(resource);
                }
            }

            log.info("{}: dirty read time: {}", tx.getTimestamp(), System.currentTimeMillis() - last);
        }


        String value = keyValueRepository.readStashedValue(tx, data);
        delay(OPERATION_THINKING_TIME);
        return value;
    }


    public void insert(DBTransaction tx, DBInsertData d) throws Exception {
        keyValueRepository.insert(tx, d);
    }

    public void insertIC3(DBTransaction tx, DBInsertData d) throws Exception {
        keyValueRepository.insertIC3(tx, d);
    }

    public void update(DBTransaction tx, DBWriteData d) throws Exception {
        keyValueRepository.write(tx, d);
    }

    public void updateIC3(DBTransaction tx, DBWriteData d) throws Exception {
        keyValueRepository.writeIC3(tx, d);
    }

    public void remove(DBTransaction tx, DBDeleteData d) throws Exception {
        keyValueRepository.remove(tx, d);
    }
    public void removeIC3(DBTransaction tx, DBDeleteData d) throws Exception {
        keyValueRepository.removeIC3(tx, d);
    }

    public int lastId(String table) throws Exception {
        return keyValueRepository.lastId(table);
    }
    // todo the logic for IC3 commit like OCC
    public boolean preCommitIC3(DBTransaction tx) throws Exception {
        return true;
    }
}



