package edgelab.retryFreeDB;

import edgelab.proto.Config;
import edgelab.proto.Data;
import edgelab.proto.LockData;
import edgelab.proto.TransactionId;
import edgelab.proto.Result;
import edgelab.proto.RetryFreeDBServerGrpc;
import edgelab.retryFreeDB.repo.concurrencyControl.DBTransaction;
import edgelab.retryFreeDB.repo.concurrencyControl.IC3Piece;
import edgelab.retryFreeDB.repo.storage.DTO.DBData;
import edgelab.retryFreeDB.repo.storage.DTO.DBDeleteData;
import edgelab.retryFreeDB.repo.storage.DTO.DBInsertData;
import edgelab.retryFreeDB.repo.storage.DTO.DBTransactionData;
import edgelab.retryFreeDB.repo.storage.DTO.DBWriteData;
import edgelab.retryFreeDB.repo.concurrencyControl.ConcurrencyControl;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
public class RetryFreeDBServer {
    private final Server server;
    private final int port;
    public RetryFreeDBServer(int port, ServerBuilder<?> serverBuilder,String postgresAddress, String postgresPort, String[] tables) throws Exception {
        this.server = serverBuilder
                .maxInboundMessageSize(Integer.MAX_VALUE)
                .addService(new RetryFreeDBService(postgresAddress, postgresPort, tables))
                .build();
        this.port = port;
    }

    public static void main(String[] args) throws IOException, InterruptedException, Exception {
        int port = Integer.parseInt(args[0]);
        String postgresAddress = args[1];
        String postgresPort = args[2];
        String[] tables = args[3].split(",");
//      SERVER
        RetryFreeDBServer server = new RetryFreeDBServer(port, ServerBuilder.forPort(port), postgresAddress, postgresPort, tables);
        server.start();
        server.blockUntilShutdown();
    }

    public void start() throws IOException {
        server.start();
        log.info("Server started, listening on " + port);
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }


    @Slf4j
    public static class RetryFreeDBService extends RetryFreeDBServerGrpc.RetryFreeDBServerImplBase {
        private final ConcurrencyControl repo;
        private String mode = "";
        ConcurrentHashMap<String, DBTransaction> transactions;
        ConcurrentHashMap<String, DBTransaction> interactiveTransactions;
//        Bamboo:
        Set<DBTransaction> toBeAbortedTransactions = ConcurrentHashMap.newKeySet();
        ConcurrentHashMap<String, AtomicInteger> lastIdUsed = new ConcurrentHashMap<>();
        private final AtomicLong lastTransactionId;
        private final AtomicLong lastRemovedTransaction;
        public RetryFreeDBService(String postgresAddress, String postgresPort, String[] tables) throws Exception {
            repo = new ConcurrencyControl(postgresAddress, postgresPort);
            transactions = new ConcurrentHashMap<>();
            interactiveTransactions = new ConcurrentHashMap<>();
            lastTransactionId =  new AtomicLong(0);
            lastRemovedTransaction = new AtomicLong(0);


            for (String table :
                    tables) {
                lastIdUsed.put(table, new AtomicInteger(repo.lastId(table)));
            }
            log.info("Table metadata initialization finished");

        }


        public void ic3PieceBegin(DBTransaction transaction, IC3Piece piece) throws Exception {
            transaction.setActiveIC3Piece(piece);


            if (piece.getConflicts().isEmpty())
                return;

            for (DBTransaction dependentTransaction : transaction.getIC3DepQueue()) {
                boolean waitedForPiece = false;

                for (IC3Piece dependentPiece : dependentTransaction.getIC3Pieces()) {
                    if (piece.getConflicts().contains(dependentPiece.getPieceName())) {
//                      wait for piece dependentPiece
                        while (!dependentPiece.isCommitted() && !dependentPiece.isAborted()) {
                            if (transaction.isAbort())
                                throw new Exception("transaction already aborted");

                            Thread.sleep(0, 1000);
                        }
                        waitedForPiece = true;
                    }
                }

                if (!waitedForPiece) {
//                    TODO: wait for transaction dependentTransaction
                    while (!dependentTransaction.isCommitted() && !dependentTransaction.isAbort()) {
                        if (transaction.isAbort())
                            throw new Exception("transaction already aborted");
                        Thread.sleep(0, 1000);
                    }
                }

            }

            piece.start();
            // ready to execute
        }


        public boolean ic3PieceCommit(DBTransaction tx, IC3Piece piece) throws Exception {
            tx.startCommitting();
            acquireLockOnIC3Sets(piece);

            boolean validated = IC3Validation(tx, piece);
//                    Write Phase
            if (validated) {
                repo.updateIC3Piece(tx, piece);
                piece.finish();
                releaseLocksOnIC3Sets(piece);
            }
            else  {
                tx.finishCommitting();
                piece.finish();
                releaseLocksOnIC3Sets(piece);
                log.error("Could not commit piece  {}:{} - occ validation failed - rollbacked", tx.getTimestamp(), piece);
                return false;
            }

            tx.finishCommitting();
            return true;

        }

        private boolean IC3Validation(DBTransaction tx, IC3Piece piece) {
            long from = tx.getTimestampValue();
            long to = this.lastTransactionId.get();

            for (long i = from + 1 ; i <= to ; i ++) {
                if (!transactions.containsKey(Long.toString(i)))
                    continue;

                DBTransaction other = transactions.get(Long.toString(i));
                if (Objects.equals(tx.getTimestamp(), other.getTimestamp()))
                    continue;


                for (IC3Piece otherPiece : other.getIC3Pieces()) {
                    if (!otherPiece.isStarted())
                        break;

                    if (otherPiece.isOCCValidated()) {
                        for (String key : otherPiece.getWriteSet().keySet()) {
                            if (piece.getReadSet().contains(key)) {
                                log.error("Could not validate tx:piece : {}:{}, {} | {} because of other tx :{},  {} | {}. Conflict on read key: {}", tx, piece, tx.getOCCReadSet(), tx.getOCCWriteSet(), other, other.getOCCReadSet(), other.getOCCWriteSet(), key);
                                return false;
                            }
                            if (!otherPiece.isFinished() && piece.getWriteSet().containsKey(key)) {
                                log.error("Could not validate tx:piece : {}:{}, {} | {} because of other tx :{},  {} | {}. Conflict on write key: {}", tx, piece, tx.getOCCReadSet(), tx.getOCCWriteSet(), other, other.getOCCReadSet(), other.getOCCWriteSet(), key);
                                return false;
                            }
                        }
                    }
                }
            }
            piece.OCCValidate();
            return true;
        }

        private void acquireLockOnIC3Sets(IC3Piece piece) {
            List<String> toLock = piece.getToLock();
            for (String key : toLock) {
                ReentrantLock lock = OCCValidationLockMap.computeIfAbsent(key, k -> new ReentrantLock());
                lock.lock();
            }
        }
        private void releaseLocksOnIC3Sets(IC3Piece piece) {
            List<String> toUnlock = piece.getToLock();
            for (String key : toUnlock) {
                ReentrantLock lock = OCCValidationLockMap.get(key);
                if (lock != null && lock.isHeldByCurrentThread()) {
                    lock.unlock();
                }
            }
        }

        @Override
        public void beginTransaction(Data request, StreamObserver<Result> responseObserver) {
           Result result = beginTransaction(request);

           responseObserver.onNext(result);
           responseObserver.onCompleted();
        }

        public Result beginTransaction(Data request) {
            try  {
                DBTransaction transaction = getTransaction(request);

                return Result.newBuilder()
                        .setStatus(true)
                        .setMessage(transaction.getTimestamp()).build();
            }
            catch (SQLException ex) {
                log.info("db error: couldn't connect/commit,  {}", ex.getMessage());
                return Result.newBuilder()
                        .setStatus(false)
                        .setMessage("b error: couldn't connect/commit,  {}").build();
            }
        }

        public DBTransaction getTransaction(Data request) throws SQLException {
            long startTime = System.nanoTime();

            String transactionId;
            if ( request.getTransactionId().isEmpty()  || this.mode.equals("occ")) // for occ we always increment
                transactionId = String.valueOf(lastTransactionId.incrementAndGet());
            else
                transactionId = request.getTransactionId();

            DBTransaction transaction = new DBTransaction(transactionId);
            if (request.getInteractiveTransaction()) {
                transaction.setInteractiveTransaction(true);
                interactiveTransactions.put(transactionId, transaction);
            }

            repo.initTransaction(transaction);
            transactions.put(transactionId, transaction);
            transaction.finishInitiation(startTime);

            return transaction;
        }

        private DBData deserilizeDataToDBData(Data request) {
            DBData d = DBTransactionData.deserializeData(request);
            if (d instanceof DBInsertData && ((DBInsertData) d).getRecordId().isEmpty()) {
                ((DBInsertData) d).setRecordId(Integer.toString(lastIdUsed.get(d.getTable()).incrementAndGet()));
            }
            return d;
        }

        public boolean isTransactionInvalid(DBTransaction tx){
            if (tx == null) {
                log.info("transaction does not exist");
                return true;
            }

            if (tx.isAbort()) {
                log.info("{}: Transaction already aborted", tx);
                try {
                    abortTransaction(tx);
                } catch (Exception ignored) {
                }
                return true;
            }
            return false;
        }

        private void abortTransaction(DBTransaction tx) throws Exception {
            if (tx.isRolledBack())
                return;
            tx.startRollingBack();
            log.info("{}: needs to be aborted Try to abort.", tx);
            log.info("abort size: {}", toBeAbortedTransactions.size());
            try {
                repo.rollback(tx, toBeAbortedTransactions);
                transactions.remove(tx.getTimestamp());
                log.warn("{}, Transaciton rollbacked", tx);
            } catch (Exception e) {
                log.error("Could not abort the transaction {}:{}", tx, e);
                throw e;
            }
            toBeAbortedTransactions.remove(tx);
            tx.finishRollingBack();
        }

        public Result lock(Data request) {
            DBTransaction tx = transactions.get(request.getTransactionId());
            return lock(tx, request);
        }


        public Result lock(DBTransaction tx, Data request) {
            if (isTransactionInvalid(tx))
                return Result.newBuilder().setStatus(false).setMessage("Could not lock - tx is invalid").build();

            tx.startLocking();
            DBData d = deserilizeDataToDBData(request);

            if (d != null) {
                try {
                    repo.lock(tx,  toBeAbortedTransactions, d);
                    tx.finishLocking();
                    if (d instanceof DBInsertData)
                        return Result.newBuilder().setStatus(true).setMessage(((DBInsertData) d).getRecordId()).build();
                    else
                        return Result.newBuilder().setStatus(true).setMessage("done").build();
                } catch (Exception e) {
                    log.error(e.getMessage());
                }
            }

            tx.finishLocking();
            return Result.newBuilder().setStatus(false).setMessage("Could not lock").build();
        }

        public Result lock(DBTransaction tx, LockData request) {
            if (isTransactionInvalid(tx))
                return Result.newBuilder().setStatus(false).setMessage("Could not lock - tx is invalid").build();

            tx.startLocking();

            try {
//                if (Objects.equals(this.mode, "occ")) {
//                    delayMicroseconds(10);
//                }
                repo.lock(tx,  toBeAbortedTransactions, request);
                tx.finishLocking();
                return Result.newBuilder().setStatus(true).setMessage("done").build();
            } catch (Exception e) {
                log.error(e.getMessage());
            }

            tx.finishLocking();
            return Result.newBuilder().setStatus(false).setMessage("Could not lock").build();
        }

        public Result tryLock(DBTransaction tx, Data request) {
            if (isTransactionInvalid(tx))
                return Result.newBuilder().setStatus(false).setMessage("Could not lock - tx is invalid").build();

            tx.startLocking();
            DBData d = deserilizeDataToDBData(request);

            if (d != null) {
                try {
                    repo.tryLock(tx, d);
                    tx.finishLocking();
                    if (d instanceof DBInsertData)
                        return Result.newBuilder().setStatus(true).setMessage(((DBInsertData) d).getRecordId()).build();
                    else
                        return Result.newBuilder().setStatus(true).setMessage("done").build();
                } catch (Exception e) {
                    log.info(e.getMessage());
                }
            }

            tx.finishLocking();
            return Result.newBuilder().setStatus(false).setMessage("Could not try lock").build();
        }

        public Result atomicLock(DBTransaction tx, String tableName, String key, List<String> values) {
            if (isTransactionInvalid(tx))
                return Result.newBuilder().setStatus(false).setMessage("Could not lock - tx is invalid").build();

            tx.startLocking();


            try {
                repo.atomicLock(tx, tableName, values, toBeAbortedTransactions);
                tx.finishLocking();
                return Result.newBuilder().setStatus(true).setMessage("done").build();
            } catch (Exception e) {
                log.info(e.getMessage());
            }

            tx.finishLocking();
            return Result.newBuilder().setStatus(false).setMessage("Could not try lock").build();
        }


        @Override
        public void lock(Data request, StreamObserver<Result> responseObserver) {
            Result result =  lock(request);
            responseObserver.onNext(result);
            responseObserver.onCompleted();
        }
        @Override
        public void unlock(Data request, StreamObserver<Result> responseObserver) {
            Result result = unlock(request);
            responseObserver.onNext(result);
            responseObserver.onCompleted();

        }

        public Result unlock(Data request) {
            DBTransaction tx = transactions.get(request.getTransactionId());
            return unlock(request, tx);
        }

        public Result unlock(Data request, DBTransaction tx) {
            if (isTransactionInvalid(tx))
                return Result.newBuilder().setStatus(false).setMessage("Could not unlock - tx is invalid").build();

            tx.startUnlocking();
            DBData d = deserilizeDataToDBData(request);
            if (d != null) {
                try {
                    repo.unlock(tx, d, toBeAbortedTransactions);
                    tx.finishUnlocking();

                    if (d instanceof DBInsertData)
                        return Result.newBuilder().setStatus(true).setMessage(((DBInsertData) d).getRecordId()).build();
                    else
                        return Result.newBuilder().setStatus(true).setMessage("done").build();
                } catch (Exception e) {
                    log.info("{}: could not unlock {}", tx.getTimestamp(), e.getMessage());
                }
                tx.finishUnlocking();
            }
            return Result.newBuilder().setStatus(false).setMessage("Could not unlock").build();
        }


        @Override
        public void lockAndUpdate(Data request, StreamObserver<Result> responseObserver) {

        }


        @Override
        public void update(Data request, StreamObserver<Result> responseObserver) {
            Result result = update(request);
            responseObserver.onNext(result);
            responseObserver.onCompleted();
        }

        public Result update(Data request) {

            DBTransaction tx = transactions.get(request.getTransactionId());
            return update(tx, request);
        }

        public Result update(DBTransaction tx, Data request) {

            if (isTransactionInvalid(tx))
                return Result.newBuilder().setStatus(false).setMessage("Could not update - tx is invalid").build();

            tx.startIO();
            DBData d = deserilizeDataToDBData(request);

            Result result;
            if (d != null) {
                log.info("{}: update on {}", tx, request.getKey());
                result = updateDBDataOnRepo(d, tx);
            }
            else {
                result = Result.newBuilder().setStatus(false).setMessage("Could not deserialize data").build();
            }

            tx.finishIO();
            return result;
        }

        private Result updateDBDataOnRepo(DBData d, DBTransaction tx) {
            try {
                if (d instanceof DBDeleteData)
                    repo.remove(tx, (DBDeleteData) d);
                else if (d instanceof DBWriteData)
                    repo.update(tx, (DBWriteData) d);
                else if(d instanceof DBInsertData)
                    repo.insert(tx, (DBInsertData) d);
                else
                    return Result.newBuilder().putAllReturns(repo.get(tx, d)).setStatus(true).setMessage("done").build();
                return Result.newBuilder().setStatus(true).setMessage("done").build();
            }
            catch (Exception ex) {
                tx.finishIO();
                return Result.newBuilder().setStatus(false).setMessage("Could not perform update: " + ex.getMessage()).build();
            }
        }

        private boolean isTransactionInvalid(String request, StreamObserver<Result> responseObserver) {
            if (!checkTransactionExists(request, responseObserver))
                return true;
            else return checkTransactionAlreadyAborted(request, responseObserver);
        }

        private void abortTransactionsThatNeedsToBeAborted() {
            while(true) {
                if (!interactiveTransactions.isEmpty()) {
//                    log.info("waiting");
//                    try {
//                        Thread.sleep(1);
//                    } catch (InterruptedException e) {
//                        throw new RuntimeException(e);
//                    }
//                } else {
                    for (String tx : interactiveTransactions.keySet()) {
                        try {
                            DBTransaction transaction = interactiveTransactions.get(tx);
                            if (transaction.isDeadLock()) {
                                transaction.setAbort();
                                toBeAbortedTransactions.add(transaction);
                                abortTransaction(transaction);
                                interactiveTransactions.remove(tx);
                                log.info("{}: Forced abort transaction because of deadlock", tx);
                            }
                        } catch (Exception e) {
                            log.error("Could not abort interactive tx: {}", e.getMessage());
                        }
                    }
                }
            }
        }
        private boolean checkTransactionAlreadyAborted(String transactionId, StreamObserver<Result> responseObserver) {
//            if (toBeAbortedTransactions.contains(tx)) {
//                log.info("{}: tx is wounded before. Try to abort.", tx);
//                rollBackTransactionWithoutInitialCheck(tx, responseObserver, false);
//                toBeAbortedTransactions.remove(tx);
//                return true;
//            }
            DBTransaction tx = transactions.get(transactionId);
            if (tx.isAbort()) {
//            if (!repo.isValid(tx.getConnection()) || toBeAbortedTransactions.contains(tx)) {
                log.info("{}: Transaction already aborted", tx);
                responseObserver.onNext(Result.newBuilder().setStatus(false).setMessage("transaction already aborted").build());
                responseObserver.onCompleted();
                return true;
            }
            return false;
        }

        private boolean checkTransactionExists(String request, StreamObserver<Result> responseObserver) {
            if (!transactions.containsKey(request)) {
                log.info("{}: No transaction with this id exists!", request);
                responseObserver.onNext(Result.newBuilder().setStatus(false).setMessage("no transaction with this id").build());
                responseObserver.onCompleted();
                return false;
            }
            return true;
        }

        @Override
        public void commitTransaction(TransactionId transactionId, StreamObserver<Result> responseObserver) {
            Result result = commitTransaction(transactionId);
            responseObserver.onNext(result);
            responseObserver.onCompleted();
        }
        public Result commitTransaction(TransactionId transactionId) {
            DBTransaction tx = transactions.get(transactionId.getId());
            return commitTransaction(tx);
        }
        public static void delayMicroseconds(long micros) {
            long startTime = System.nanoTime();
            long endTime = startTime + (micros * 1000);
            while (System.nanoTime() < endTime) {
                // Busy-wait
            }
        }

        public Result commitTransaction(DBTransaction tx) {

            if (isTransactionInvalid(tx))
                return Result.newBuilder().setStatus(false).setMessage("Could not commit - tx is invalid").build();

            try {
                tx.startCommitting();
                repo.release(tx, toBeAbortedTransactions);

                if(Objects.equals(this.mode, "occ")) {
//                    Validation Phase
                    acquireLockOnOCCSets(tx);
                    boolean validated = OCCValidation(tx);
//                    Write Phase
                    if (validated) {
                        OCCUpdate(tx);
                        tx.OCCFinish();
                        releaseLocksOnOCCSets(tx);
                    }
                    else  {
                        tx.finishCommitting();
                        tx.setAbort();

                        releaseLocksOnOCCSets(tx);
                        isTransactionInvalid(tx);
                        return Result.newBuilder().setStatus(false).setMessage("Could not commit - occ validation failed - rollbacked").build();
                    }

                }
                else if (Objects.equals(this.mode, "ic3")) {

                    IC3WaitForDependentTransactions(tx);
                    IC3Update(tx);
                    tx.commit();
                }



                removeTransaction(tx);
                tx.finishCommitting();
                log.warn("{}, Transaciton commited, total waiting time: {}", tx.getTimestamp(), tx.getWaitingTime());
                return Result.newBuilder().setStatus(true)
                        .putReturns("waiting_time", String.valueOf(tx.getWaitingTime()))
                        .putReturns("io_time", String.valueOf(tx.getIoTime()))
                        .putReturns("locking_time", String.valueOf(tx.getLockingTime()))
                        .putReturns("retiring_time", String.valueOf(tx.getRetiringTime()))
                        .putReturns("initiation_time", String.valueOf(tx.getInitiationTime()))
                        .putReturns("unlocking_time", String.valueOf(tx.getUnlockingTime()))
                        .putReturns("committing_time", String.valueOf(tx.getCommittingTime()))
                        .putReturns("waiting_for_others_time", String.valueOf(tx.getWaitingForOthersTime()))
                        .putReturns("rolling_back_time", String.valueOf(tx.getRollingBackTime()))
                        .setMessage("released").build();
            } catch (Exception e) {
                return Result.newBuilder().setStatus(false).setMessage("Could not release the locks/commit" + e.getMessage()).build();
            }
        }

        private void IC3Update(DBTransaction tx) throws Exception {
            for (String key : tx.getOCCWriteSet().keySet()) {
                DBData data = tx.getOCCWriteSet().get(key);
                this.repo.updateIC3(tx, data, key);
            }
        }

        private void IC3WaitForDependentTransactions(DBTransaction tx) throws Exception {
            for (DBTransaction dependentTransaction : tx.getIC3DepQueue()) {
                while (!dependentTransaction.isCommitted() && !dependentTransaction.isAbort()) {
                    if (tx.isAbort())
                        throw new Exception("transaction already aborted");
                    Thread.sleep(0, 1000);
                }

            }
        }


        private void removeTransaction(DBTransaction tx) {
//            We need to remove transactions in an ascending order for OCC to be able to validate with ts > t_current
            if (Long.parseLong(tx.getTimestamp()) > 1000 + lastRemovedTransaction.get()) {
                transactions.remove(Long.toString(lastRemovedTransaction.incrementAndGet()));
            }

        }

        private void OCCUpdate(DBTransaction tx) throws Exception {
            for (String key : tx.getOCCWriteSet().keySet()) {
                DBData data = tx.getOCCWriteSet().get(key);
                this.repo.updateOCC(tx, data);
            }
        }


        private final Map<String, ReentrantLock> OCCValidationLockMap = new ConcurrentHashMap<>();
        private void acquireLockOnOCCSets(DBTransaction tx) {
            List<String> toLock = tx.getOCCtoLock();
            for (String key : toLock) {
                ReentrantLock lock = OCCValidationLockMap.computeIfAbsent(key, k -> new ReentrantLock());
                lock.lock();
            }
        }

        private boolean OCCValidation(DBTransaction tx) {

            long from = tx.getTimestampValue();
            long to = this.lastTransactionId.get();

            for (long i = from + 1 ; i <= to ; i ++) {
                if (!transactions.containsKey(Long.toString(i)))
                    continue;

                DBTransaction other = transactions.get(Long.toString(i));
                if (other != null && other.isOCCValidated()) {
                    if (!Objects.equals(tx.getTimestamp(), other.getTimestamp())) {
                        for (String key : other.getOCCWriteSet().keySet()) {
                            if (tx.getOCCReadSet().contains(key)) {
                                log.error("Could not validate tx : {}, {} | {} because of other tx :{},  {} | {}. Conflict on read key: {}", tx, tx.getOCCReadSet(), tx.getOCCWriteSet(), other, other.getOCCReadSet(), other.getOCCWriteSet(), key);
                                return false;
                            }
                            if (!other.isOCCFinished() && tx.getOCCWriteSet().containsKey(key)) {
                                log.error("Could not validate tx : {}, {} | {} because of other tx :{},  {} | {}. Conflict on write key: {}", tx, tx.getOCCReadSet(), tx.getOCCWriteSet(), other, other.getOCCReadSet(), other.getOCCWriteSet(), key);
                                return false;
                            }
                        }
                    }
                }
            }
            tx.OCCValidate();
            return true;
        }

        private void releaseLocksOnOCCSets(DBTransaction tx) {
            List<String> toUnlock = tx.getOCCtoLock();
            for (String key : toUnlock) {
                ReentrantLock lock = OCCValidationLockMap.get(key);
                if (lock != null && lock.isHeldByCurrentThread()) {
//                   OCCValidationLockMap.compute(key, (k, v) -> {
//                        if (v != null && v.getHoldCount() == 1 && v.isHeldByCurrentThread()) {
//                            return null;
//                        }
//                        return v;
//                    });

                    lock.unlock();
                }
            }
        }


        @Override
        public void rollBackTransaction(TransactionId transactionId, StreamObserver<Result> responseObserver) {
            Result result = rollBackTransaction(transactionId);
            responseObserver.onNext(result);
            responseObserver.onCompleted();
        }

        public Result rollBackTransaction(TransactionId transactionId) {
            DBTransaction tx = transactions.get(transactionId.getId());
            return rollBackTransaction(tx);
        }




        public Result rollBackTransaction(DBTransaction tx) {
            if (tx == null)
                return Result.newBuilder().setStatus(false).setMessage("Could not rollback - tx is invalid").build();
            try {
                abortTransaction(tx);
                log.warn("{}, Transaciton rollbacked, total waiting time: {}", tx, tx.getWaitingTime());

                return Result.newBuilder().putReturns("waiting_time", String.valueOf(tx.getWaitingTime()))
                        .putReturns("io_time", String.valueOf(tx.getIoTime()))
                        .putReturns("locking_time", String.valueOf(tx.getLockingTime()))
                        .putReturns("retiring_time", String.valueOf(tx.getRetiringTime()))
                        .putReturns("initiation_time", String.valueOf(tx.getInitiationTime()))
                        .putReturns("unlocking_time", String.valueOf(tx.getUnlockingTime()))
                        .putReturns("committing_time", String.valueOf(tx.getCommittingTime()))
                        .putReturns("waiting_for_others_time", String.valueOf(tx.getWaitingForOthersTime()))
                        .putReturns("rolling_back_time", String.valueOf(tx.getRollingBackTime()))
                        .setMessage("rollbacked").build();
            } catch (Exception e) {
                tx.finishRollingBack();
                return Result.newBuilder().setStatus(false).setMessage("Could not rollback and release the locks").build();
            }
        }


        @Override
        public void bambooRetireLock(Data request, StreamObserver<Result> responseObserver) {
            Result result = bambooRetireLock(request);
            responseObserver.onNext(result);
            responseObserver.onCompleted();
        }

        public Result bambooRetireLock(Data request) {
            DBTransaction tx = transactions.get(request.getTransactionId());
            return bambooRetireLock(request, tx);
        }

        public Result bambooRetireLock(Data request, DBTransaction tx) {
            if (isTransactionInvalid(tx))
                return Result.newBuilder().setStatus(false).setMessage("Could not retire - tx is invalid").build();

            Result result;
            DBData d = deserilizeDataToDBData(request);
            if (d != null) {
                tx.startRetireLock();
                try {
                    repo.retireLock(tx, d);
                    result = Result.newBuilder().setStatus(true).setMessage("done").build();
                } catch (Exception e) {
                    result = Result.newBuilder().setStatus(false).setMessage("Could not retire " + e.getMessage()).build();
                }
                tx.finishRetireLock();
            }
            else {
                result = Result.newBuilder().setStatus(false).setMessage("Could not retire:  deserilize problem ").build();
            }
            return result;
        }

        @Override
        public void bambooWaitForCommit(TransactionId transactionId, StreamObserver<Result> responseObserver) {
            Result result = bambooWaitForCommit(transactionId);
            responseObserver.onNext(result);
            responseObserver.onCompleted();
        }

        public Result bambooWaitForCommit(TransactionId transactionId) {
            DBTransaction tx = transactions.get(transactionId.getId());
            return bambooWaitForCommit(tx);
        }

        public Result bambooWaitForCommit(DBTransaction tx) {
            if (isTransactionInvalid(tx))
                return Result.newBuilder().setStatus(false).setMessage("Could bamboo wait for commit - tx is invalid").build();

            tx.startWaitingForOthers();
            Result result;
            try {
                log.info("{}: wait for commit", tx);
                tx.waitForCommit();
                result = Result.newBuilder().setStatus(true).setMessage("done").build();
            } catch (Exception e) {
                try {
                    abortTransaction(tx); // tx is aborted could not wait anymore
                } catch (Exception ignored) {
                }
                result = Result.newBuilder().setStatus(false).setMessage("Error:" + e.getMessage()).build();
            }
            tx.finishWaitingForOthers();
            return result;
        }


        @Override
        public void setConfig(Config config, StreamObserver<Result> responseObserver) {
           Result result = setConfig(config);
            responseObserver.onNext(result);
            responseObserver.onCompleted();
        }

        public Result setConfig(Config config) {
            Map<String, String> configMap = config.getValuesMap();
            try {
                if (configMap.containsKey("mode")) {
                    this.mode = configMap.get("mode");
                    ConcurrencyControl.setMode(configMap.get("mode"));
                    log.info("2pl mode is set to :{}", configMap.get("mode"));
//                    if (Objects.equals(this.mode, "slw")) {
////                        ExecutorService executor = Executors.newSingleThreadExecutor();
////                        executor.submit(this::abortTransactionsThatNeedsToBeAborted);
//                    }
                }

                if (configMap.containsKey("operationDelay")) {
                    ConcurrencyControl.OPERATION_THINKING_TIME = Integer.parseInt(configMap.get("operationDelay"));
                    log.info("operation thinking time is set to {}", Integer.parseInt(configMap.get("operationDelay")));
                }

                return Result.newBuilder().setStatus(true).setMessage("done").build();
            }
            catch (Exception e) {
                return Result.newBuilder().setStatus(false).setMessage("Error:" + e.getMessage()).build();
            }
        }




//
//        @Override
//        public void get(Key request, StreamObserver<Result> responseObserver) {
////            try {
////                String value = repo.find(request.getKey());
////                responseObserver.onNext(Result.newBuilder().setSuccess(true).setMessage(value).build());
////                responseObserver.onCompleted();
////            } catch (RocksDBException e) {
////                responseObserver.onError(e);
////            }
//        }
//
//        @Override
//        public void delete(Key request, StreamObserver<Result> responseObserver) {
////            try {
////                repo.delete(request.getKey());
////                responseObserver.onNext(Result.newBuilder().setSuccess(true).setMessage("deleted").build());
////                responseObserver.onCompleted();
////            } catch (RocksDBException e) {
////                responseObserver.onError(e);
////            }
//        }
//
//        @Override
//        public void clear(StreamObserver<Result> responseObserver) {
////            try {
////                repo.clear();
////                responseObserver.onNext(Result.newBuilder().setSuccess(true).setMessage("cleared").build());
////                responseObserver.onCompleted();
////            } catch (RocksDBException | IOException e) {
////                responseObserver.onError(e);
////            }
//        }
//
//        @Override
//        public void batch(Values request, StreamObserver<Result> responseObserver) {
//////            if (repo.isChangingMemory())
//////                responseObserver.onError(new RocksDBException("Changing memory"));
////            try {
//////                repo.saveBatchWithGRPCInterrupt(request.getValuesMap());
////                repo.saveBatch(request.getValuesMap());
////                responseObserver.onNext(Result.newBuilder().setSuccess(true).setMessage("saved the batch").build());
////                responseObserver.onCompleted();
////            } catch (RocksDBException e) {
////                responseObserver.onError(e);
////            }
//////            catch (InterruptedException e) {
//////                responseObserver.onError(e);
//////            }
//
//        }
//
//
//        @Override
//        public void changeMemory(Size size, StreamObserver<Result> responseObserver) {
////            try {
////                repo.changeMemorySize(size.getValue());
////                log.info("Memory changed to  {}", size.getValue());
////                responseObserver.onNext(Result.newBuilder().setSuccess(true).setMessage("Changed the memory size to " + size.getValue()).build());
////                responseObserver.onCompleted();
////            } catch (RocksDBException | IOException e) {
////                responseObserver.onError(e);
////            }
//        }


    }

}
