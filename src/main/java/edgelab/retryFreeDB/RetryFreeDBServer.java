package edgelab.retryFreeDB;

import edgelab.proto.Config;
import edgelab.proto.Data;
import edgelab.proto.Empty;
import edgelab.proto.TransactionId;
import edgelab.proto.Result;
import edgelab.proto.RetryFreeDBServerGrpc;
import edgelab.retryFreeDB.repo.concurrencyControl.DBTransaction;
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
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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
        ConcurrentHashMap<String, DBTransaction> transactions;
//        Bamboo:
        Set<DBTransaction> toBeAbortedTransactions = ConcurrentHashMap.newKeySet();
        ConcurrentHashMap<String, AtomicInteger> lastIdUsed = new ConcurrentHashMap<>();
        private final AtomicLong lastTransactionId;
        public RetryFreeDBService(String postgresAddress, String postgresPort, String[] tables) throws Exception {
            repo = new ConcurrencyControl(postgresAddress, postgresPort);
            transactions = new ConcurrentHashMap<>();
            lastTransactionId =  new AtomicLong(0);

            for (String table :
                    tables) {
                lastIdUsed.put(table, new AtomicInteger(repo.lastId(table)));
            }
            log.info("Table metadata initialization finished");

            ExecutorService executor = Executors.newSingleThreadExecutor();
            executor.submit(this::abortTransactionsThatNeedsToBeAborted);

        }

        @Override
        public void beginTransaction(Empty request, StreamObserver<Result> responseObserver) {
            try  {
                long startTime = System.currentTimeMillis();


                long transactionId = lastTransactionId.incrementAndGet();
                DBTransaction transaction = new DBTransaction(Long.toString(transactionId));
                repo.initTransaction(transaction);

                transactions.put(Long.toString(transactionId), transaction);

                transaction.finishInitiation(startTime);
                responseObserver.onNext(Result.newBuilder()
                                        .setStatus(true)
                                        .setMessage(Long.toString(transactionId)).build());
                responseObserver.onCompleted();
            }
            catch (SQLException ex) {
                log.info("db error: couldn't connect/commit,  {}", ex.getMessage());
                responseObserver.onNext(Result.newBuilder()
                        .setStatus(false)
                        .setMessage("b error: couldn't connect/commit,  {}").build());
                responseObserver.onCompleted();
            }
        }

        private DBData deserilizeDataToDBData(Data request) {
            DBData d = DBTransactionData.deserializeData(request);
            if (d instanceof DBInsertData && ((DBInsertData) d).getRecordId().isEmpty()) {
                ((DBInsertData) d).setRecordId(Integer.toString(lastIdUsed.get(d.getTable()).incrementAndGet()));
            }
            return d;
        }

        @Override
        public void lock(Data request, StreamObserver<Result> responseObserver) {
            if (isTransactionInvalid(request.getTransactionId(), responseObserver)) return;


            DBTransaction tx = transactions.get(request.getTransactionId());
            DBData d = deserilizeDataToDBData(request);

            if (d != null) {
                try {
                    repo.lock(tx,  toBeAbortedTransactions, d);
                    if (d instanceof DBInsertData)
                        responseObserver.onNext(Result.newBuilder().setStatus(true).setMessage(((DBInsertData) d).getRecordId()).build());
                    else
                        responseObserver.onNext(Result.newBuilder().setStatus(true).setMessage("done").build());
                    responseObserver.onCompleted();
                } catch (Exception e) {
//                    if (e.getSQLState().equals(Postgres.DEADLOCK_ERROR)) {
//                        responseObserver.onNext(Result.newBuilder().setStatus(false).setMessage("deadlock").build());
//                    }
//                    else
                    log.error(e.getMessage());
                    responseObserver.onNext(Result.newBuilder().setStatus(false).setMessage("Could not lock").build());
                    responseObserver.onCompleted();
                }
            }

        }
        @Override
        public void unlock(Data request, StreamObserver<Result> responseObserver) {
            if (isTransactionInvalid(request.getTransactionId(), responseObserver)) return;

            DBTransaction tx = transactions.get(request.getTransactionId());
            DBData d = deserilizeDataToDBData(request);
            if (d != null) {
                tx.startUnlocking();
                try {
                    repo.unlock(tx, d, toBeAbortedTransactions);
                    if (d instanceof DBInsertData)
                        responseObserver.onNext(Result.newBuilder().setStatus(true).setMessage(((DBInsertData) d).getRecordId()).build());
                    else
                        responseObserver.onNext(Result.newBuilder().setStatus(true).setMessage("done").build());
                    responseObserver.onCompleted();
                } catch (Exception e) {
                    responseObserver.onNext(Result.newBuilder().setStatus(false).setMessage("Could not unlock").build());
                    responseObserver.onCompleted();
                }
                tx.finishUnlocking();
            }
        }



        @Override
        public void lockAndUpdate(Data request, StreamObserver<Result> responseObserver) {
            if (isTransactionInvalid(request.getTransactionId(), responseObserver)) return;

            DBTransaction tx = transactions.get(request.getTransactionId());
            DBData d = deserilizeDataToDBData(request);

            if (d != null) {
                try {
                    repo.lock(tx, toBeAbortedTransactions, d);
                } catch (Exception e) {
//                    if (e.getSQLState().equals(Postgres.DEADLOCK_ERROR))
//                        responseObserver.onNext(Result.newBuilder().setStatus(false).setMessage("deadlock").build());
//                    else
                        responseObserver.onNext(Result.newBuilder().setStatus(false).setMessage("Could not lock").build());
                    responseObserver.onCompleted();
                    return;
                }
                updateDBDataOnRepo(responseObserver, d, tx);
            }
            else {
                responseObserver.onNext(Result.newBuilder().setStatus(false).setMessage("Could not deserialize data").build());
                responseObserver.onCompleted();
            }
        }


        @Override
        public void update(Data request, StreamObserver<Result> responseObserver) {
            if (isTransactionInvalid(request.getTransactionId(), responseObserver)) return;

            DBTransaction tx = transactions.get(request.getTransactionId());
            DBData d = deserilizeDataToDBData(request);

            if (d != null) {
                log.info("{}: update on {}", tx, request.getKey());
                updateDBDataOnRepo(responseObserver, d, tx);
            }
            else {
                responseObserver.onNext(Result.newBuilder().setStatus(false).setMessage("Could not deserialize data").build());
                responseObserver.onCompleted();
            }
        }

        private void updateDBDataOnRepo(StreamObserver<Result> responseObserver, DBData d, DBTransaction tx) {
            try {
                String result = "done";
                tx.startIO();
                if (d instanceof DBDeleteData)
                    repo.remove(tx, (DBDeleteData) d);
                else if (d instanceof DBWriteData)
                    repo.update(tx, (DBWriteData) d);
                else if(d instanceof DBInsertData)
                    repo.insert(tx, (DBInsertData) d);
                else
                    result = repo.get(tx, d);
                tx.finishIO();
                responseObserver.onNext(Result.newBuilder().setStatus(true).setMessage(result).build());
                responseObserver.onCompleted();
            }
            catch (Exception ex) {
                tx.finishIO();
                responseObserver.onNext(Result.newBuilder().setStatus(false).setMessage("Could not perform update: " + ex.getMessage()).build());
                responseObserver.onCompleted();
            }
        }

        private boolean isTransactionInvalid(String request, StreamObserver<Result> responseObserver) {
            if (!checkTransactionExists(request, responseObserver))
                return true;
            else return checkTransactionAlreadyAborted(request, responseObserver);
        }

        private void abortTransactionsThatNeedsToBeAborted() {
            while(true) {
                if (toBeAbortedTransactions.isEmpty()) {
//                    log.info("waiting");
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                } else {

                    for (DBTransaction tx : toBeAbortedTransactions) {
                        log.info("{}: needs to be aborted Try to abort.", tx);
                        log.info("abort size: {}", toBeAbortedTransactions.size());
                        Connection conn = tx.getConnection();
                        try {
                            repo.rollback(tx, toBeAbortedTransactions);
                            transactions.remove(tx);
                            log.warn("{}, Transaciton rollbacked", tx);
                        } catch (Exception e) {
                            log.error("Could not abort the transaction {}:{}", tx, e);
                        }
                        toBeAbortedTransactions.remove(tx);
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
            if (isTransactionInvalid(transactionId.getId(), responseObserver)) return;

            DBTransaction tx = transactions.get(transactionId.getId());
            try {
                tx.startCommitting();
                repo.release(tx, toBeAbortedTransactions);
                transactions.remove(transactionId.getId());
                tx.finishCommitting();
                log.warn("{}, Transaciton commited, total waiting time: {}", transactionId.getId(), tx.getWaitingTime());
                responseObserver.onNext(Result.newBuilder().setStatus(true)
                        .putReturns("waiting_time", String.valueOf(tx.getWaitingTime()))
                        .putReturns("io_time", String.valueOf(tx.getIoTime()))
                        .putReturns("locking_time", String.valueOf(tx.getLockingTime()))
                        .putReturns("retiring_time", String.valueOf(tx.getRetiringTime()))
                        .putReturns("initiation_time", String.valueOf(tx.getInitiationTime()))
                        .putReturns("unlocking_time", String.valueOf(tx.getUnlockingTime()))
                        .putReturns("committing_time", String.valueOf(tx.getCommittingTime()))
                        .putReturns("waiting_for_others_time", String.valueOf(tx.getWaitingForOthersTime()))
                        .setMessage("released").build());
                responseObserver.onCompleted();
            } catch (Exception e) {
                responseObserver.onNext(Result.newBuilder().setStatus(false).setMessage("Could not release the locks").build());
                responseObserver.onCompleted();
            }
        }


        @Override
        public void rollBackTransaction(TransactionId transactionId, StreamObserver<Result> responseObserver) {
            if (isTransactionInvalid(transactionId.getId(), responseObserver)) return;

            rollBackTransactionWithoutInitialCheck(transactionId.getId(), responseObserver, true);
        }

        private void rollBackTransactionWithoutInitialCheck(String transactionId, StreamObserver<Result> responseObserver, boolean finalStatus) {
            DBTransaction tx = transactions.get(transactionId);
            try {
                repo.rollback(tx, toBeAbortedTransactions);
                transactions.remove(tx.toString());
                log.warn("{}, Transaciton rollbacked, total waiting time: {}", tx, tx.getWaitingTime());
                responseObserver.onNext(Result.newBuilder().setStatus(finalStatus)
                        .putReturns("waiting_time", String.valueOf(tx.getWaitingTime())).setMessage("rollbacked").build());
                responseObserver.onCompleted();
            } catch (Exception e) {

                responseObserver.onNext(Result.newBuilder().setStatus(false).setMessage("Could not rollback and release the locks").build());
                responseObserver.onCompleted();
            }
        }


        @Override
        public void bambooRetireLock(Data request, StreamObserver<Result> responseObserver) {
            if (isTransactionInvalid(request.getTransactionId(), responseObserver)) return;

            DBTransaction tx = transactions.get(request.getTransactionId());
            DBData d = deserilizeDataToDBData(request);
            if (d != null) {
                tx.startRetireLock();
                try {
                    repo.retireLock(tx, d);
                    responseObserver.onNext(Result.newBuilder().setStatus(true).setMessage("done").build());
                    responseObserver.onCompleted();
                } catch (Exception e) {
                    responseObserver.onNext(Result.newBuilder().setStatus(false).setMessage("Could not retire " + e.getMessage()).build());
                    responseObserver.onCompleted();
                }
                tx.finishRetireLock();
            }


        }
        @Override
        public void bambooWaitForCommit(TransactionId transactionId, StreamObserver<Result> responseObserver) {
            if (isTransactionInvalid(transactionId.getId(), responseObserver)) return;

            DBTransaction tx = transactions.get(transactionId.getId());
            tx.startWaitingForOthers();
            try {
                log.info("{}: wait for commit", tx);
                tx.waitForCommit();
                responseObserver.onNext(Result.newBuilder().setStatus(true).setMessage("done").build());
                responseObserver.onCompleted();
            } catch (Exception e) {
                responseObserver.onNext(Result.newBuilder().setStatus(false).setMessage("Error:" + e.getMessage()).build());
                responseObserver.onCompleted();
            }
            tx.finishWaitingForOthers();
        }


        @Override
        public void setConfig(Config config, StreamObserver<Result> responseObserver) {
            Map<String, String> configMap = config.getValuesMap();
            try {
                if (configMap.containsKey("mode")) {
                    ConcurrencyControl.setMode(configMap.get("mode"));
                    log.info("2pl mode is set to :{}", configMap.get("mode"));
                }

                if (configMap.containsKey("operationDelay")) {
                    ConcurrencyControl.OPERATION_THINKING_TIME = Integer.parseInt(configMap.get("operationDelay"));
                    log.info("operation thinking time is set to {}", Integer.parseInt(configMap.get("operationDelay")));
                }

                responseObserver.onNext(Result.newBuilder().setStatus(true).setMessage("done").build());
                responseObserver.onCompleted();
            }
            catch (Exception e) {
                responseObserver.onNext(Result.newBuilder().setStatus(false).setMessage("Error:" + e.getMessage()).build());
                responseObserver.onCompleted();
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

    public static class RetryFreeDBServiceWrapper {
        private final RetryFreeDBService service;

        public RetryFreeDBServiceWrapper(RetryFreeDBService service) {
            this.service = service;
        }

        // Helper to simplify StreamObserver-based calls
        private <T> T executeWithObserver(java.util.function.Consumer<StreamObserver<T>> consumer) {
            CompletableFuture<T> future = new CompletableFuture<>();
            StreamObserver<T> observer = new StreamObserver<>() {
                @Override
                public void onNext(T value) {
                    future.complete(value);
                }

                @Override
                public void onError(Throwable t) {
                    future.completeExceptionally(t);
                }

                @Override
                public void onCompleted() {
                    // No-op
                }
            };
            consumer.accept(observer);
            try {
                return future.get(); // Block and wait for the response
            } catch (Exception e) {
                throw new RuntimeException("gRPC method call failed", e);
            }
        }

        // Wrapper for beginTransaction
        public Result beginTransaction(Empty empty) {
            return executeWithObserver(observer -> service.beginTransaction(empty, observer));
        }

        // Wrapper for lock
        public Result lock(Data request) {
            return executeWithObserver(observer -> service.lock(request, observer));
        }

        // Wrapper for unlock
        public Result unlock(Data request) {
            return executeWithObserver(observer -> service.unlock(request, observer));
        }

        // Wrapper for lockAndUpdate
        public Result lockAndUpdate(Data request) {
            return executeWithObserver(observer -> service.lockAndUpdate(request, observer));
        }

        // Wrapper for update
        public Result update(Data request) {
            return executeWithObserver(observer -> service.update(request, observer));
        }

        // Wrapper for commitTransaction
        public Result commitTransaction(TransactionId transactionId) {
            return executeWithObserver(observer -> service.commitTransaction(transactionId, observer));
        }

        // Wrapper for rollBackTransaction
        public Result rollBackTransaction(TransactionId transactionId) {
            return executeWithObserver(observer -> service.rollBackTransaction(transactionId, observer));
        }

        // Wrapper for bambooRetireLock
        public Result bambooRetireLock(Data request) {
            return executeWithObserver(observer -> service.bambooRetireLock(request, observer));
        }

        // Wrapper for bambooWaitForCommit
        public Result bambooWaitForCommit(TransactionId transactionId) {
            return executeWithObserver(observer -> service.bambooWaitForCommit(transactionId, observer));
        }

        // Wrapper for setConfig
        public Result setConfig(Config config) {
            return executeWithObserver(observer -> service.setConfig(config, observer));
        }
    }
}
