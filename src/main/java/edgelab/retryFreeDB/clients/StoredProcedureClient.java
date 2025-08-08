package edgelab.retryFreeDB.clients;

import edgelab.proto.Config;
import edgelab.proto.Data;
import edgelab.proto.LockData;
import edgelab.proto.LockType;
import edgelab.proto.Result;
import edgelab.retryFreeDB.RetryFreeDBServer;
import edgelab.retryFreeDB.benchmark.Performance;
import edgelab.retryFreeDB.repo.concurrencyControl.DBTransaction;
import edgelab.retryFreeDB.repo.concurrencyControl.IC3Piece;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static edgelab.retryFreeDB.RetryFreeDBConfiguration.DELETE_TYPE;
import static edgelab.retryFreeDB.RetryFreeDBConfiguration.INSERT_TYPE;
import static edgelab.retryFreeDB.RetryFreeDBConfiguration.READ_TYPE;
import static edgelab.retryFreeDB.RetryFreeDBConfiguration.WRITE_TYPE;

@Slf4j
public class StoredProcedureClient extends Client{

//    private final RetryFreeDBServerGrpc.RetryFreeDBServerBlockingStub blockingStub;

//    private final ManagedChannel channel;
    private Map<String, Set<String>> locks = new ConcurrentHashMap<>();

    private final String mode;

    private RetryFreeDBServer.RetryFreeDBService dbService;

    private final Random random = new Random(123);

    @Getter
    public  Map<String, Long> pidAccessed = new ConcurrentHashMap<>();

    public StoredProcedureClient(String postgresAddress, String postgresPort, String[] tables, String mode) throws Exception {
        this.dbService = new RetryFreeDBServer.RetryFreeDBService(postgresAddress, postgresPort, tables);
        this.mode = mode;
    }
    static class Pair {
        int key;
        int value;

        Pair(int key, int value) {
            this.key = key;
            this.value = value;
        }
    }


    public static void main(String args[]) throws Exception {

        Client client = new StoredProcedureClient("./rocksdb/db", String.valueOf(-1), "Listings,Items,Players".split(","), "slw");

        Map<String, HashSet<String>> hotRecords = new HashMap<>();

        client.addListing("1", "5", 1, "");
        client.buyListing("1", "100001", "");


//          client.buyListing("2", "1");
//        client.addListingSLW("1", "3", 1);
//          client.buyListingSLW("2", "1000");
//          client.simulateDeadlock();
//          client.test();
//        client.addListing("7", "7", 280);
//        client.readItem(List.of("10", "11", "12"));

//        client.TPCC_payment("1", "2", 10.0f, "1", "2", "1");


//
//        client.TPCC_newOrder("1", "2", "3", "3", "1",
//                new int[]{1,2,3},
//                new int[]{1,1,1},
//                new int[]{1,2,3});


    }

    private void test() {
//        String tx1 = dbService.beginTransaction(Data.newBuilder().build()).getMessage();
//        String tx2 = dbService.beginTransaction(Data.newBuilder().build()).getMessage();
//        String tx3 = dbService.beginTransaction(Data.newBuilder().build()).getMessage();
//        String tx4 = dbService.beginTransaction(Data.newBuilder().build()).getMessage();
//        String tx5 = dbService.beginTransaction(Data.newBuilder().build()).getMessage();
//        String tx6 = dbService.beginTransaction(Data.newBuilder().build()).getMessage();
//
//        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(5);
//        executor.submit(()->  {
////            lock(tx3, "Players", "PId", "10", WRITE_TYPE);
////            retireLock(tx3, "Players", "PId", "10");
//
//            try {
//                lock(tx5, "Players", "PId", "10", WRITE_TYPE);
//
//                retireLock(tx5, "Players", "PId", "10");
//
//                lock(tx6, "Players", "PId", "10", WRITE_TYPE);
//                retireLock(tx6, "Players", "PId", "10");
//
//
//                lock(tx4, "Players", "PId", "10", WRITE_TYPE);
//            } catch (Exception e) {
//                throw new RuntimeException(e);
//            }
//
//
//        });
//        delay();
//
//        executor.submit(() -> {
//            try {
//                waitForCommit(tx5);
//            } catch (Exception e) {
//                throw new RuntimeException(e);
//            }
//            commit(tx5);
//        });
//
//        executor.submit(() -> {
//            try {
//                waitForCommit(tx4);
//            } catch (Exception e) {
//                throw new RuntimeException(e);
//            }
//            commit(tx4);
//        });
//
//        executor.submit(() -> {
//            try {
//                waitForCommit(tx3);
//            } catch (Exception e) {
//                throw new RuntimeException(e);
//            }
//            commit(tx3);
//        });
//
//        executor.submit(() -> {
//            try {
//                waitForCommit(tx6);
//            } catch (Exception e) {
//                throw new RuntimeException(e);
//            }
//            commit(tx6);
//        });
//        executor.submit(()->  lock(tx2, "Players", "PId", "11", WRITE_TYPE));
//        delay();
//
//        executor.submit(()->  {
//            lock(tx2, "Players", "PId", "10", READ_TYPE);
//            log.info("{} now can access 10", tx2);
//            log.info("{} value of player 10: {}", tx2, read(tx2, "Players", "PId", "10"));
//        });
//        delay();
//
//        executor.submit(()->  {
//            lock(tx1, "Players", "PId", "11", READ_TYPE);
//            log.info("{} now can access 11", tx1);
////            lock(tx1, )
//
//        });
//        delay();

//        executor.submit(()-> rollback(tx1));
//        commit(tx2);
//        commit(tx1);

    }
    public TransactionResult TPCC_newOrder(String warehouseId, String districtId, String customerId, String orderLineCount, String allLocals, int[] itemIDs, int[] supplierWarehouseIDs, int[] orderQuantities, String txId) {
        switch (mode) {
            case "ww" , "occ"-> {
                return TPCC_newOrderWW(warehouseId, districtId, customerId, orderLineCount, allLocals, itemIDs, supplierWarehouseIDs, orderQuantities, txId);
            }
            case "slw" -> {
                return TPCC_newOrderSLW(warehouseId, districtId, customerId, orderLineCount, allLocals, itemIDs, supplierWarehouseIDs, orderQuantities, txId);
            }
            case "ordered" -> {
                return TPCC_newOrderOrdered(warehouseId, districtId, customerId, orderLineCount, allLocals, itemIDs, supplierWarehouseIDs, orderQuantities, txId);
            }
            case "bamboo" -> {
                return TPCC_newOrderBamboo(warehouseId, districtId, customerId, orderLineCount, allLocals, itemIDs, supplierWarehouseIDs, orderQuantities, txId);
            }
            case "ic3" -> {
                return TPCC_newOrderIC3(warehouseId, districtId, customerId, orderLineCount, allLocals, itemIDs, supplierWarehouseIDs, orderQuantities, txId);
            }
            default -> {
                log.error("Does not support this mode: {}", mode);
                return new TransactionResult();
            }
        }
    }

    public TransactionResult TPCC_newOrderIC3(String warehouseId, String districtId, String customerId, String orderLineCount, String allLocals, int[] itemIDs, int[] supplierWarehouseIDs, int[] orderQuantities, String txId) {
        TransactionResult res = new TransactionResult();
        DBTransaction dbTransaction = null;
        try {
            dbTransaction = dbService.getTransaction(Data.newBuilder().setTransactionId(txId).build());
            res.setTxId(dbTransaction.getTimestamp());
        } catch (SQLException e) {
            return res;
        }
        if (dbTransaction != null) {
            try {


                IC3Piece piece1 = new IC3Piece(Set.of(
                        "newOrder:p1",
                        "payment:p1"
                ), "newOrder:p1", dbTransaction.getTimestampValue());
                dbTransaction.addIC3Piece(piece1);

                IC3Piece piece2 = new IC3Piece(Set.of(), "newOrder:p2", dbTransaction.getTimestampValue());
                dbTransaction.addIC3Piece(piece2);

                IC3Piece piece3 = new IC3Piece(Set.of(), "newOrder:p3", dbTransaction.getTimestampValue());
                dbTransaction.addIC3Piece(piece3);



                String listingRecordId = null;

                boolean piece1Res = false;
                int retry = 0;
                Map<String, String> district = null;
                while (!piece1Res) {
                    if (retry > 100) {
                        rollback(dbTransaction);
                        return res;
                    }


                    dbService.ic3PieceBegin(dbTransaction, piece1);
//              Get customer
                    String customerIdKey = "c_w_id,c_d_id,c_id";
                    String customerIdValue = warehouseId + "," + districtId + "," + customerId;
                    Map<String, String> customer = read(dbTransaction, "customer", customerIdKey, customerIdValue);
                    if (customer.isEmpty()) {
                        dbService.ic3PieceCommit(dbTransaction, piece1);
                        throw new Exception("customer does not exists");
                    }


//              Get warehouse
                    Map<String, String> warehouse = read(dbTransaction, "warehouse", "w_id", warehouseId);
                    if (warehouse.isEmpty()) {
                        dbService.ic3PieceCommit(dbTransaction, piece1);
                        throw new Exception("warehouse does not exists");
                    }


//              Get district
                    district = read(dbTransaction, "district", "d_w_id,d_id", warehouseId + "," + districtId);
                    if (district.isEmpty()) {
                        dbService.ic3PieceCommit(dbTransaction, piece1);
                        throw new Exception("district does not exists");
                    }

//              Update district
                    String d_next_o_id = String.valueOf(Integer.parseInt(district.get("d_next_o_id")) + 1);
                    write(dbTransaction, "district", "d_w_id,d_id", warehouseId + "," + districtId, "d_next_o_id", d_next_o_id);

                    piece1Res = dbService.ic3PieceCommit(dbTransaction, piece1);


                    retry++;
                }




                boolean piece2Res = false;
                retry = 0;
                while (!piece2Res) {
                    if (retry > 100) {
                        rollback(dbTransaction);
                        return res;
                    }
                    dbService.ic3PieceBegin(dbTransaction, piece2);
                    //              Insert OpenOrder
                    String openOrderId = warehouseId + "," + districtId + "," + district.get("d_next_o_id");
                    insert(dbTransaction, "oorder", customerId + "," +
                                    "null" + "," +
                                    orderLineCount + "," +
                                    allLocals + "," +
                                    "'" + new Timestamp(System.currentTimeMillis()) + "'"
                            ,openOrderId);
                    piece2Res =  dbService.ic3PieceCommit(dbTransaction, piece2);
                    retry++;
                }


                boolean piece3Res = false;
                retry = 0;
                while (!piece3Res) {
                    if (retry > 100) {
                        rollback(dbTransaction);
                        return res;
                    }
                    dbService.ic3PieceBegin(dbTransaction, piece3);


                    //              Insert NewOrder
                    String newOrderId = warehouseId + "," + districtId + "," + district.get("d_next_o_id");
                    insertLock(dbTransaction, "new_order", newOrderId);
                    insert(dbTransaction, "new_order", "", newOrderId);

                    piece3Res =  dbService.ic3PieceCommit(dbTransaction, piece3);
                    retry++;
                }




                for (int ol_number = 1; ol_number <= Integer.parseInt(orderLineCount); ol_number++) {
                    String ol_supply_w_id = String.valueOf(supplierWarehouseIDs[ol_number - 1]);
                    String ol_i_id = String.valueOf(itemIDs[ol_number - 1]);
                    int ol_quantity = orderQuantities[ol_number - 1];


                    IC3Piece piece4 = new IC3Piece(Set.of(), "newOrder:p4" + ol_number, dbTransaction.getTimestampValue());
                    dbTransaction.addIC3Piece(piece4);

                    IC3Piece piece5 = new IC3Piece(Set.of(), "newOrder:p5" + ol_number, dbTransaction.getTimestampValue());
                    dbTransaction.addIC3Piece(piece5);

                    IC3Piece piece6 = new IC3Piece(Set.of(), "newOrder:p6" + ol_number, dbTransaction.getTimestampValue());
                    dbTransaction.addIC3Piece(piece6);

                    IC3Piece piece7 = new IC3Piece(Set.of(), "newOrder:p7" + ol_number, dbTransaction.getTimestampValue());
                    dbTransaction.addIC3Piece(piece7);

                    boolean piece4Res = false;
                    retry = 0;
                    Map<String, String> item = null;
                    while (!piece4Res) {
                        if (retry > 100) {
                            rollback(dbTransaction);
                            return res;
                        }
                        dbService.ic3PieceBegin(dbTransaction, piece4);

                        //                    Get Item Price
                        item = read(dbTransaction, "item", "i_id", ol_i_id);

                        piece4Res =  dbService.ic3PieceCommit(dbTransaction, piece4);
                        retry++;
                    }




                    float ol_amount = ol_quantity * Float.parseFloat(item.get("i_price"));
//                    Get Stock
                    lock(dbTransaction, "stock", "s_w_id,s_i_id", ol_supply_w_id + "," + ol_i_id, READ_TYPE);
                    Map<String, String> stock = null;

                    boolean piece5Res = false;
                    retry = 0;
                    while (!piece5Res) {
                        if (retry > 100) {
                            rollback(dbTransaction);
                            return res;
                        }
                        dbService.ic3PieceBegin(dbTransaction, piece5);

                        //                    Get Item Price
                        stock = read(dbTransaction, "stock", "s_w_id,s_i_id", ol_supply_w_id + "," + ol_i_id);

                        piece5Res =  dbService.ic3PieceCommit(dbTransaction, piece5);
                        retry++;
                    }



                    String ol_dist_info = stock.get(getDistInfoKey(districtId));
                    String newStockQuantity;
                    if (Integer.parseInt(stock.get("s_quantity")) - ol_quantity >= 10) {
                        newStockQuantity = String.valueOf(Integer.parseInt(stock.get("s_quantity")) - ol_quantity);
                    } else {
                        newStockQuantity = String.valueOf(Integer.parseInt(stock.get("s_quantity")) - ol_quantity + 91);
                    }
                    int stockRemoteCountIncrement = 0;
                    if (!ol_supply_w_id.equals(warehouseId)) {
                        stockRemoteCountIncrement += 1;
                    }



                    boolean piece6Res = false;
                    retry = 0;
                    while (!piece6Res) {
                        if (retry > 100) {
                            rollback(dbTransaction);
                            return res;
                        }
                        dbService.ic3PieceBegin(dbTransaction, piece6);

                        //                    Insert OrderLine
                        String orderLineId = warehouseId + "," + districtId + "," + district.get("d_next_o_id") + "," + ol_number;
                        insert(dbTransaction, "order_line", ol_i_id + "," + "'" + new Timestamp(System.currentTimeMillis()) + "'"
                                + "," + ol_amount + "," + ol_supply_w_id + "," + ol_quantity + "," + ol_dist_info, orderLineId);

                        piece6Res =  dbService.ic3PieceCommit(dbTransaction, piece6);
                        retry++;
                    }



//                    Update Stock
                    lock(dbTransaction, "stock", "s_w_id,s_i_id", ol_supply_w_id + "," + ol_i_id, WRITE_TYPE);

                    Map<String, String> newStockValues = new HashMap<>();
                    newStockValues.put( "s_quantity", newStockQuantity);
                    newStockValues.put( "s_ytd", String.valueOf(Float.parseFloat(stock.get("s_ytd")) + ol_quantity));
                    newStockValues.put( "s_order_cnt", String.valueOf(Integer.parseInt(stock.get("s_order_cnt")) + 1));
                    newStockValues.put("s_remote_cnt", String.valueOf(Integer.parseInt(stock.get("s_remote_cnt")) + stockRemoteCountIncrement));


                    boolean piece7Res = false;
                    retry = 0;
                    while (!piece7Res) {
                        if (retry > 100) {
                            rollback(dbTransaction);
                            return res;
                        }
                        dbService.ic3PieceBegin(dbTransaction, piece7);

                        //                    Update Stock
                        write(dbTransaction,"stock", "s_w_id,s_i_id", ol_supply_w_id + "," + ol_i_id, newStockValues);

                        piece7Res =  dbService.ic3PieceCommit(dbTransaction, piece7);
                        retry++;
                    }
                }

                getCommitResult(dbTransaction, res);

            } catch (Exception e) {
                log.error(e.getMessage());
                rollback(dbTransaction);
                return res;
            }
        }

        return res;
    }


    public TransactionResult TPCC_newOrderWW(String warehouseId, String districtId, String customerId, String orderLineCount, String allLocals, int[] itemIDs, int[] supplierWarehouseIDs, int[] orderQuantities, String txId) {
        TransactionResult res = new TransactionResult();
        DBTransaction dbTransaction = null;
        try {
            dbTransaction = dbService.getTransaction(Data.newBuilder().setTransactionId(txId).build());
            res.setTxId(dbTransaction.getTimestamp());
        } catch (SQLException e) {
            return res;
        }
        if (dbTransaction != null) {
            try {
//              Get customer
                String customerIdKey = "c_w_id,c_d_id,c_id";
                String customerIdValue = warehouseId + "," + districtId + "," + customerId;
                lock(dbTransaction, "customer", customerIdKey, customerIdValue, READ_TYPE);
                Map<String, String> customer = read(dbTransaction, "customer", customerIdKey, customerIdValue);
                if (customer.isEmpty())
                    throw new Exception("customer does not exists");


//              Get warehouse
                lock(dbTransaction, "warehouse", "w_id", warehouseId, READ_TYPE);
                Map<String, String> warehouse = read(dbTransaction, "warehouse", "w_id", warehouseId);
                if (warehouse.isEmpty())
                    throw new Exception("warehouse does not exists");



//              Get district
                lock(dbTransaction, "district", "d_w_id,d_id", warehouseId + "," + districtId, READ_TYPE);
                Map<String, String> district = read(dbTransaction, "district", "d_w_id,d_id", warehouseId + "," + districtId);
                if (district.isEmpty())
                    throw new Exception("district does not exists");

//              Update district
                lock(dbTransaction, "district", "d_w_id,d_id", warehouseId + "," + districtId, WRITE_TYPE);
                String d_next_o_id = String.valueOf(Integer.parseInt(district.get("d_next_o_id")) + 1);
                write(dbTransaction, "district", "d_w_id,d_id", warehouseId + "," + districtId, "d_next_o_id", d_next_o_id);

//              Insert OpenOrder
                String openOrderId = warehouseId + "," + districtId + "," + district.get("d_next_o_id");
                insertLock(dbTransaction, "oorder", openOrderId);
                insert(dbTransaction, "oorder", customerId + "," +
                                "null" + "," +
                                orderLineCount + "," +
                                allLocals + "," +
                                "'" + new Timestamp(System.currentTimeMillis()) + "'"
                        ,openOrderId);

//              Insert NewOrder
                String newOrderId = warehouseId + "," + districtId + "," + district.get("d_next_o_id");
                insertLock(dbTransaction, "new_order", newOrderId);
                insert(dbTransaction, "new_order", "", newOrderId);

                for (int ol_number = 1; ol_number <= Integer.parseInt(orderLineCount); ol_number++) {
                    String ol_supply_w_id = String.valueOf(supplierWarehouseIDs[ol_number - 1]);
                    String ol_i_id = String.valueOf(itemIDs[ol_number - 1]);
                    int ol_quantity = orderQuantities[ol_number - 1];

//                    Get Item Price
                    lock(dbTransaction, "item", "i_id", String.valueOf(ol_i_id), READ_TYPE);
                    Map<String, String> item = read(dbTransaction, "item", "i_id", ol_i_id);

                    float ol_amount = ol_quantity * Float.parseFloat(item.get("i_price"));
//                    Get Stock
                    lock(dbTransaction, "stock", "s_w_id,s_i_id", ol_supply_w_id + "," + ol_i_id, READ_TYPE);
                    Map<String, String> stock = read(dbTransaction, "stock", "s_w_id,s_i_id", ol_supply_w_id + "," + ol_i_id);

                    String ol_dist_info = stock.get(getDistInfoKey(districtId));
                    String newStockQuantity;
                    if (Integer.parseInt(stock.get("s_quantity")) - ol_quantity >= 10) {
                        newStockQuantity = String.valueOf(Integer.parseInt(stock.get("s_quantity")) - ol_quantity);
                    } else {
                        newStockQuantity = String.valueOf(Integer.parseInt(stock.get("s_quantity")) - ol_quantity + 91);
                    }
                    int stockRemoteCountIncrement = 0;
                    if (!ol_supply_w_id.equals(warehouseId)) {
                        stockRemoteCountIncrement += 1;
                    }


//                    Insert OrderLine
                    String orderLineId = warehouseId + "," + districtId + "," + district.get("d_next_o_id") + "," + ol_number;
                    insertLock(dbTransaction, "order_line", orderLineId);
                    insert(dbTransaction, "order_line", ol_i_id + "," + "'" + new Timestamp(System.currentTimeMillis()) + "'"
                            + "," + ol_amount + "," + ol_supply_w_id + "," + ol_quantity + "," + ol_dist_info, orderLineId);

//                    Update Stock
                    lock(dbTransaction, "stock", "s_w_id,s_i_id", ol_supply_w_id + "," + ol_i_id, WRITE_TYPE);

                    Map<String, String> newStockValues = new HashMap<>();
                    newStockValues.put( "s_quantity", newStockQuantity);
                    newStockValues.put( "s_ytd", String.valueOf(Float.parseFloat(stock.get("s_ytd")) + ol_quantity));
                    newStockValues.put( "s_order_cnt", String.valueOf(Integer.parseInt(stock.get("s_order_cnt")) + 1));
                    newStockValues.put("s_remote_cnt", String.valueOf(Integer.parseInt(stock.get("s_remote_cnt")) + stockRemoteCountIncrement));
                    write(dbTransaction,"stock", "s_w_id,s_i_id", ol_supply_w_id + "," + ol_i_id, newStockValues);
                }

                getCommitResult(dbTransaction, res);

            } catch (Exception e) {
                log.error(e.getMessage());
                rollback(dbTransaction);
                return res;
            }
        }

        return res;
    }


    private void getCommitResult(DBTransaction tx, TransactionResult res) {
        Result commitResult = commit(tx);
        res.setSuccess(commitResult.getStatus());
        if (commitResult.getStatus())
            res.setMetrics(commitResult.getReturnsMap());
    }

    public TransactionResult TPCC_newOrderBamboo(String warehouseId, String districtId, String customerId, String orderLineCount, String allLocals, int[] itemIDs, int[] supplierWarehouseIDs, int[] orderQuantities, String txId) {
        TransactionResult res = new TransactionResult();
        DBTransaction dbTransaction = null;
        try {
            dbTransaction = dbService.getTransaction(Data.newBuilder().setTransactionId(txId).build());
            res.setTxId(dbTransaction.getTimestamp());
        } catch (SQLException e) {
            return res;
        }
        if (dbTransaction != null) {
            try {
//              Get customer
                String customerIdKey = "c_w_id,c_d_id,c_id";
                String customerIdValue = warehouseId + "," + districtId + "," + customerId;
                lock(dbTransaction, "customer", customerIdKey, customerIdValue, READ_TYPE);
                Map<String, String> customer = read(dbTransaction, "customer", customerIdKey, customerIdValue);
                if (customer.isEmpty())
                    throw new Exception("customer does not exists");


//              Get warehouse
                lock(dbTransaction, "warehouse", "w_id", warehouseId, READ_TYPE);
                Map<String, String> warehouse = read(dbTransaction, "warehouse", "w_id", warehouseId);
                if (warehouse.isEmpty())
                    throw new Exception("warehouse does not exists");



//              Get district
                lock(dbTransaction, "district", "d_w_id,d_id", warehouseId + "," + districtId, READ_TYPE);
                Map<String, String> district = read(dbTransaction, "district", "d_w_id,d_id", warehouseId + "," + districtId);
                if (district.isEmpty())
                    throw new Exception("district does not exists");

//              Update district
                lock(dbTransaction, "district", "d_w_id,d_id", warehouseId + "," + districtId, WRITE_TYPE);
                String d_next_o_id = String.valueOf(Integer.parseInt(district.get("d_next_o_id")) + 1);
                write(dbTransaction, "district", "d_w_id,d_id", warehouseId + "," + districtId, "d_next_o_id", d_next_o_id);

                retireLock(dbTransaction, "district", "d_w_id,d_id", warehouseId + "," + districtId);

//              Insert OpenOrder
                String openOrderId = warehouseId + "," + districtId + "," + district.get("d_next_o_id");
                insertLock(dbTransaction, "oorder", openOrderId);
                insert(dbTransaction, "oorder", customerId + "," +
                                "null" + "," +
                                orderLineCount + "," +
                                allLocals + "," +
                                "'" + new Timestamp(System.currentTimeMillis()) + "'"
                        ,openOrderId);

//              Insert NewOrder
                String newOrderId = warehouseId + "," + districtId + "," + district.get("d_next_o_id");
                insertLock(dbTransaction, "new_order", newOrderId);
                insert(dbTransaction, "new_order", "", newOrderId);

                for (int ol_number = 1; ol_number <= Integer.parseInt(orderLineCount); ol_number++) {
                    String ol_supply_w_id = String.valueOf(supplierWarehouseIDs[ol_number - 1]);
                    String ol_i_id = String.valueOf(itemIDs[ol_number - 1]);
                    int ol_quantity = orderQuantities[ol_number - 1];

//                    Get Item Price
                    lock(dbTransaction, "item", "i_id", String.valueOf(ol_i_id), READ_TYPE);
                    Map<String, String> item = read(dbTransaction, "item", "i_id", ol_i_id);

                    float ol_amount = ol_quantity * Float.parseFloat(item.get("i_price"));
//                    Get Stock
                    lock(dbTransaction, "stock", "s_w_id,s_i_id", ol_supply_w_id + "," + ol_i_id, READ_TYPE);
                    Map<String, String> stock = read(dbTransaction, "stock", "s_w_id,s_i_id", ol_supply_w_id + "," + ol_i_id);

                    String ol_dist_info = stock.get(getDistInfoKey(districtId));
                    String newStockQuantity;
                    if (Integer.parseInt(stock.get("s_quantity")) - ol_quantity >= 10) {
                        newStockQuantity = String.valueOf(Integer.parseInt(stock.get("s_quantity")) - ol_quantity);
                    } else {
                        newStockQuantity = String.valueOf(Integer.parseInt(stock.get("s_quantity")) - ol_quantity + 91);
                    }
                    int stockRemoteCountIncrement = 0;
                    if (!ol_supply_w_id.equals(warehouseId)) {
                        stockRemoteCountIncrement += 1;
                    }


//                    Insert OrderLine
                    String orderLineId = warehouseId + "," + districtId + "," + district.get("d_next_o_id") + "," + ol_number;
                    insertLock(dbTransaction, "order_line", orderLineId);
                    insert(dbTransaction, "order_line", ol_i_id + "," + "'" + new Timestamp(System.currentTimeMillis()) + "'"
                            + "," + ol_amount + "," + ol_supply_w_id + "," + ol_quantity + "," + ol_dist_info, orderLineId);

//                    Update Stock
                    lock(dbTransaction, "stock", "s_w_id,s_i_id", ol_supply_w_id + "," + ol_i_id, WRITE_TYPE);

                    Map<String, String> newStockValues = new HashMap<>();
                    newStockValues.put( "s_quantity", newStockQuantity);
                    newStockValues.put( "s_ytd", String.valueOf(Float.parseFloat(stock.get("s_ytd")) + ol_quantity));
                    newStockValues.put( "s_order_cnt", String.valueOf(Integer.parseInt(stock.get("s_order_cnt")) + 1));
                    newStockValues.put("s_remote_cnt", String.valueOf(Integer.parseInt(stock.get("s_remote_cnt")) + stockRemoteCountIncrement));
                    write(dbTransaction,"stock", "s_w_id,s_i_id", ol_supply_w_id + "," + ol_i_id, newStockValues);
                }

                waitForCommit(dbTransaction);
                getCommitResult(dbTransaction, res);

            } catch (Exception e) {
                log.error(e.getMessage());
                rollback(dbTransaction);
                return res;
            }
        }

        return res;
    }
    public TransactionResult TPCC_newOrderSLW(String warehouseId, String districtId, String customerId, String orderLineCount, String allLocals, int[] itemIDs, int[] supplierWarehouseIDs, int[] orderQuantities, String txId) {
        TransactionResult res = new TransactionResult();
        DBTransaction dbTransaction = null;
        try {
            dbTransaction = dbService.getTransaction(Data.newBuilder().setTransactionId(txId).build());
            res.setTxId(dbTransaction.getTimestamp());
        } catch (SQLException e) {
            return res;
        }
        if (dbTransaction != null) {
            try {
//              Get customer
                String customerIdKey = "c_w_id,c_d_id,c_id";
                String customerIdValue = warehouseId + "," + districtId + "," + customerId;
                lock(dbTransaction, "customer", customerIdKey, customerIdValue, READ_TYPE);
                Map<String, String> customer = read(dbTransaction, "customer", customerIdKey, customerIdValue);
                if (customer.isEmpty())
                    throw new Exception("customer does not exists");


//              Get warehouse
                lock(dbTransaction, "warehouse", "w_id", warehouseId, READ_TYPE);
                Map<String, String> warehouse = read(dbTransaction, "warehouse", "w_id", warehouseId);
                if (warehouse.isEmpty())
                    throw new Exception("warehouse does not exists");



//              Get district
                lock(dbTransaction, "district", "d_w_id,d_id", warehouseId + "," + districtId, WRITE_TYPE);
                Map<String, String> district = read(dbTransaction, "district", "d_w_id,d_id", warehouseId + "," + districtId);
                if (district.isEmpty())
                    throw new Exception("district does not exists");

//              Update district
                String d_next_o_id = String.valueOf(Integer.parseInt(district.get("d_next_o_id")) + 1);
                write(dbTransaction, "district", "d_w_id,d_id", warehouseId + "," + districtId, "d_next_o_id", d_next_o_id);


//              Prepare Stocks locks
                InteractiveClient.Pair[] pairs = new InteractiveClient.Pair[Integer.parseInt(orderLineCount)];
                for (int i = 0; i < Integer.parseInt(orderLineCount); i++) {
                    pairs[i] = new InteractiveClient.Pair(supplierWarehouseIDs[i], itemIDs[i]);
                }

                Arrays.sort(pairs, Comparator.comparingInt((InteractiveClient.Pair pair) -> pair.key)
                        .thenComparing(pair -> pair.value));

                for (int ol_number = 1; ol_number <= Integer.parseInt(orderLineCount); ol_number++) {
                    String ol_supply_w_id = String.valueOf(pairs[ol_number - 1].key);
                    String ol_i_id = String.valueOf(pairs[ol_number - 1].value);
                    lock(dbTransaction, "stock", "s_w_id,s_i_id", ol_supply_w_id + "," + ol_i_id, WRITE_TYPE);
                }

//                Early Release of Locks: Unlock customer, warehouse, district
                if(!Performance.INTERACTIVE) {
                    unlock(dbTransaction, "warehouse", "w_id", warehouseId);
                    unlock(dbTransaction, "district", "d_w_id,d_id", warehouseId + "," + districtId);
                    unlock(dbTransaction, "customer", customerIdKey, customerIdValue);
                }


//              Insert OpenOrder
                String openOrderId = warehouseId + "," + districtId + "," + district.get("d_next_o_id");
                insertLock(dbTransaction, "oorder", openOrderId);
                insert(dbTransaction, "oorder", customerId + "," +
                                "null" + "," +
                                orderLineCount + "," +
                                allLocals + "," +
                                "'" + new Timestamp(System.currentTimeMillis()) + "'"
                        ,openOrderId);

//              Insert NewOrder
                String newOrderId = warehouseId + "," + districtId + "," + district.get("d_next_o_id");
                insertLock(dbTransaction, "new_order", newOrderId);
                insert(dbTransaction, "new_order", "", newOrderId);



                for (int ol_number = 1; ol_number <= Integer.parseInt(orderLineCount); ol_number++) {
                    String ol_supply_w_id = String.valueOf(pairs[ol_number - 1].key);
                    String ol_i_id = String.valueOf(pairs[ol_number - 1].value);
                    int ol_quantity = orderQuantities[ol_number - 1];

//                    Get Item Price
                    lock(dbTransaction, "item", "i_id", ol_i_id, READ_TYPE);
                    Map<String, String> item = read(dbTransaction, "item", "i_id", ol_i_id);

                    float ol_amount = ol_quantity * Float.parseFloat(item.get("i_price"));
//                    Get Stock
                    Map<String, String> stock = read(dbTransaction, "stock", "s_w_id,s_i_id", ol_supply_w_id + "," + ol_i_id);

                    String ol_dist_info = stock.get(getDistInfoKey(districtId));
                    String newStockQuantity;
                    if (Integer.parseInt(stock.get("s_quantity")) - ol_quantity >= 10) {
                        newStockQuantity = String.valueOf(Integer.parseInt(stock.get("s_quantity")) - ol_quantity);
                    } else {
                        newStockQuantity = String.valueOf(Integer.parseInt(stock.get("s_quantity")) - ol_quantity + 91);
                    }
                    int stockRemoteCountIncrement = 0;
                    if (!ol_supply_w_id.equals(warehouseId)) {
                        stockRemoteCountIncrement += 1;
                    }


//                    Insert OrderLine
                    String orderLineId = warehouseId + "," + districtId + "," + district.get("d_next_o_id") + "," + ol_number;
                    insertLock(dbTransaction, "order_line", orderLineId);
                    insert(dbTransaction, "order_line", ol_i_id + "," + "'" + new Timestamp(System.currentTimeMillis()) + "'"
                            + "," + ol_amount + "," + ol_supply_w_id + "," + ol_quantity + "," + ol_dist_info, orderLineId);

//                    Update Stock
                    Map<String, String> newStockValues = new HashMap<>();
                    newStockValues.put( "s_quantity", newStockQuantity);
                    newStockValues.put( "s_ytd", String.valueOf(Float.parseFloat(stock.get("s_ytd")) + ol_quantity));
                    newStockValues.put( "s_order_cnt", String.valueOf(Integer.parseInt(stock.get("s_order_cnt")) + 1));
                    newStockValues.put("s_remote_cnt", String.valueOf(Integer.parseInt(stock.get("s_remote_cnt")) + stockRemoteCountIncrement));
                    write(dbTransaction,"stock", "s_w_id,s_i_id", ol_supply_w_id + "," + ol_i_id, newStockValues);
                }

                getCommitResult(dbTransaction, res);

            } catch (Exception e) {
                log.error(e.getMessage());
                rollback(dbTransaction);
                return res;
            }
        }

        return res;
    }

    public TransactionResult TPCC_newOrderOrdered(String warehouseId, String districtId, String customerId, String orderLineCount, String allLocals, int[] itemIDs, int[] supplierWarehouseIDs, int[] orderQuantities, String txId) {
        TransactionResult res = new TransactionResult();
        DBTransaction dbTransaction = null;
        try {
            dbTransaction = dbService.getTransaction(Data.newBuilder().setTransactionId(txId).build());
            res.setTxId(dbTransaction.getTimestamp());
        } catch (SQLException e) {
            return res;
        }
        if (dbTransaction != null) {
            try {
//              Get customer
                String customerIdKey = "c_w_id,c_d_id,c_id";
                String customerIdValue = warehouseId + "," + districtId + "," + customerId;
                String d_next_o_id;
                List<List<String>> allLocks = new ArrayList<>();
                allLocks.add(List.of("customer", customerIdKey, customerIdValue, READ_TYPE));
                allLocks.add(List.of("warehouse", "w_id", warehouseId, READ_TYPE));
                allLocks.add(List.of("district", "d_w_id,d_id", warehouseId + "," + districtId, WRITE_TYPE));

                Map<String, String> district = read(dbTransaction, "district", "d_w_id,d_id", warehouseId + "," + districtId);
                d_next_o_id = district.get("d_next_o_id");
                String openOrderId = warehouseId + "," + districtId + "," + d_next_o_id;
                allLocks.add(List.of("oorder", openOrderId, "", INSERT_TYPE));
                String newOrderId = warehouseId + "," + districtId + "," + d_next_o_id;
                allLocks.add(List.of("new_order", newOrderId, "", INSERT_TYPE));

                for (int ol_number = 1; ol_number <= Integer.parseInt(orderLineCount); ol_number++) {
                    String ol_supply_w_id = String.valueOf(supplierWarehouseIDs[ol_number - 1]);
                    String ol_i_id = String.valueOf(itemIDs[ol_number - 1]);
                    String orderLineId = warehouseId + "," + districtId + "," + d_next_o_id + "," + ol_number;


                    allLocks.add(List.of("item", "i_id", String.valueOf(ol_i_id), READ_TYPE));
                    allLocks.add(List.of("stock", "s_w_id,s_i_id", ol_supply_w_id + "," + ol_i_id, WRITE_TYPE));
                    allLocks.add(List.of("order_line", orderLineId, "", INSERT_TYPE));
                }

                allLocks.sort(Comparator.comparing(a -> (a.get(0) + a.get(1) + a.get(2))));
                Collections.reverse(allLocks);

                for (List<String> lockParams : allLocks) {
                    String table = lockParams.get(0);
                    String key = lockParams.get(1);
                    String value = lockParams.size() > 2 ? lockParams.get(2) : "";
                    String type = lockParams.get(3);

                    if (type.equals(INSERT_TYPE)) {
                        insertLock(dbTransaction, table, key);
                    } else {
                        lock(dbTransaction, table, key, value, type);
                    }
                }



                Map<String, String> customer = read(dbTransaction, "customer", customerIdKey, customerIdValue);
                if (customer.isEmpty())
                    throw new Exception("customer does not exists");


//              Get warehouse

                Map<String, String> warehouse = read(dbTransaction, "warehouse", "w_id", warehouseId);
                if (warehouse.isEmpty())
                    throw new Exception("warehouse does not exists");



//              Get district

                district = read(dbTransaction, "district", "d_w_id,d_id", warehouseId + "," + districtId);
                if (district.isEmpty())
                    throw new Exception("district does not exists");

//              Update district

                d_next_o_id = String.valueOf(Integer.parseInt(district.get("d_next_o_id")) + 1);
                write(dbTransaction, "district", "d_w_id,d_id", warehouseId + "," + districtId, "d_next_o_id", d_next_o_id);

//              Insert OpenOrder


                insert(dbTransaction, "oorder", customerId + "," +
                                "null" + "," +
                                orderLineCount + "," +
                                allLocals + "," +
                                "'" + new Timestamp(System.currentTimeMillis()) + "'"
                        ,openOrderId);

//              Insert NewOrder

                insert(dbTransaction, "new_order", "", newOrderId);

                for (int ol_number = 1; ol_number <= Integer.parseInt(orderLineCount); ol_number++) {
                    String ol_supply_w_id = String.valueOf(supplierWarehouseIDs[ol_number - 1]);
                    String ol_i_id = String.valueOf(itemIDs[ol_number - 1]);
                    int ol_quantity = orderQuantities[ol_number - 1];

//                    Get Item Price

                    Map<String, String> item = read(dbTransaction, "item", "i_id", ol_i_id);
                    float ol_amount = ol_quantity * Float.parseFloat(item.get("i_price"));
//                    Get Stock

                    Map<String, String> stock = read(dbTransaction, "stock", "s_w_id,s_i_id", ol_supply_w_id + "," + ol_i_id);

                    String ol_dist_info = stock.get(getDistInfoKey(districtId));
                    String newStockQuantity;
                    if (Integer.parseInt(stock.get("s_quantity")) - ol_quantity >= 10) {
                        newStockQuantity = String.valueOf(Integer.parseInt(stock.get("s_quantity")) - ol_quantity);
                    } else {
                        newStockQuantity = String.valueOf(Integer.parseInt(stock.get("s_quantity")) - ol_quantity + 91);
                    }
                    int stockRemoteCountIncrement = 0;
                    if (!ol_supply_w_id.equals(warehouseId)) {
                        stockRemoteCountIncrement += 1;
                    }


//                    Insert OrderLine
                    String orderLineId = warehouseId + "," + districtId + "," + district.get("d_next_o_id") + "," + ol_number;

                    insert(dbTransaction, "order_line", ol_i_id + "," + "'" + new Timestamp(System.currentTimeMillis()) + "'"
                            + "," + ol_amount + "," + ol_supply_w_id + "," + ol_quantity + "," + ol_dist_info, orderLineId);



                    Map<String, String> newStockValues = new HashMap<>();
                    newStockValues.put( "s_quantity", newStockQuantity);
                    newStockValues.put( "s_ytd", String.valueOf(Float.parseFloat(stock.get("s_ytd")) + ol_quantity));
                    newStockValues.put( "s_order_cnt", String.valueOf(Integer.parseInt(stock.get("s_order_cnt")) + 1));
                    newStockValues.put("s_remote_cnt", String.valueOf(Integer.parseInt(stock.get("s_remote_cnt")) + stockRemoteCountIncrement));
                    write(dbTransaction,"stock", "s_w_id,s_i_id", ol_supply_w_id + "," + ol_i_id, newStockValues);
                }

                getCommitResult(dbTransaction, res);

            } catch (Exception e) {
                log.error(e.getMessage());
                rollback(dbTransaction);
                return res;
            }
        }

        return res;
    }

    private String getDistInfoKey(String districtId) {
        return switch (districtId) {
            case "1" -> "s_dist_01";
            case "2" -> "s_dist_02";
            case "3" -> "s_dist_03";
            case "4" -> "s_dist_04";
            case "5" -> "s_dist_05";
            case "6" -> "s_dist_06";
            case "7" -> "s_dist_07";
            case "8" -> "s_dist_08";
            case "9" -> "s_dist_09";
            case "10" -> "s_dist_10";
            default -> null;
        };
    }


    public TransactionResult TPCC_payment(String warehouseId, String districtId, float paymentAmount, String customerWarehouseId, String customerDistrictId, String customerId, String txId) {
        switch (mode) {
            case "ww", "occ" -> {
                return TPCC_paymentWW(warehouseId, districtId, paymentAmount, customerWarehouseId, customerDistrictId, customerId, txId);
            }
            case "slw" -> {
                return TPCC_paymentSLW(warehouseId, districtId, paymentAmount, customerWarehouseId, customerDistrictId, customerId, txId);
            }
            case "ordered" -> {
                return TPCC_paymentOrdered(warehouseId, districtId, paymentAmount, customerWarehouseId, customerDistrictId, customerId, txId);
            }
            case "bamboo" -> {
                return TPCC_paymentBamboo(warehouseId, districtId, paymentAmount, customerWarehouseId, customerDistrictId, customerId, txId);
            }
            case "ic3" -> {
                return TPCC_paymentIC3(warehouseId, districtId, paymentAmount, customerWarehouseId, customerDistrictId, customerId, txId);
            }
            default -> {
                log.error("Does not support this mode: {}", mode);
                return new TransactionResult();
            }
        }
    }

    public TransactionResult TPCC_paymentIC3(String warehouseId, String districtId, float paymentAmount, String customerWarehouseId, String customerDistrictId, String customerId, String txId) {
        TransactionResult res = new TransactionResult();
        DBTransaction dbTransaction = null;
        try {
            dbTransaction = dbService.getTransaction(Data.newBuilder().setTransactionId(txId).build());
            res.setTxId(dbTransaction.getTimestamp());
        } catch (SQLException e) {
            return res;
        }
        if (dbTransaction != null) {
            try {

                IC3Piece piece1 = new IC3Piece(Set.of(
                        "newOrder:p1",
                        "payment:p1"
                ), "payment:p1", dbTransaction.getTimestampValue());
                dbTransaction.addIC3Piece(piece1);

                IC3Piece piece2 = new IC3Piece(Set.of(), "payment:p2", dbTransaction.getTimestampValue());
                dbTransaction.addIC3Piece(piece2);


                boolean piece1Res = false;
                int retry = 0;
                while (!piece1Res) {
                    if (retry > 100) {
                        rollback(dbTransaction);
                        return res;
                    }
                    dbService.ic3PieceBegin(dbTransaction, piece1);

//            Get warehouse
                    lock(dbTransaction, "warehouse", "w_id", warehouseId, READ_TYPE);
                    Map<String, String> warehouse = read(dbTransaction, "warehouse", "w_id", warehouseId);
                    if (warehouse.isEmpty()) {
                        log.info("warehouse does not exists");
                        dbService.ic3PieceCommit(dbTransaction, piece1);
                        commit(dbTransaction);
                        return res;

                    }

//            update warehouse
                    lock(dbTransaction, "warehouse", "w_id", warehouseId, WRITE_TYPE);
                    float newWarehouseBalance = Float.parseFloat(warehouse.get("w_ytd")) + paymentAmount;
                    write(dbTransaction, "warehouse", "w_id", warehouseId, "w_ytd", String.valueOf(newWarehouseBalance)) ;



//          get district
                    lock(dbTransaction, "district", "d_w_id,d_id", warehouseId + "," + districtId, READ_TYPE);
                    Map<String, String> district = read(dbTransaction, "district", "d_w_id,d_id", warehouseId + "," + districtId);
                    if (district.isEmpty()) {
                        dbService.ic3PieceCommit(dbTransaction, piece1);
                        throw new Exception("district does not exists");
                    }
                    //            Update District
                    lock(dbTransaction, "district", "d_w_id,d_id", warehouseId + "," + districtId, WRITE_TYPE);
                    float newDistrictBalance = Float.parseFloat(district.get("d_ytd")) + paymentAmount;
                    write(dbTransaction, "district", "d_w_id,d_id", warehouseId + "," + districtId, "d_ytd", String.valueOf(newDistrictBalance)) ;

                    //            Get customer
                    String customerIdKey = "c_w_id,c_d_id,c_id";
                    String customerIdValue = customerWarehouseId + "," + customerDistrictId + "," + customerId;
                    lock(dbTransaction, "customer", customerIdKey, customerIdValue, READ_TYPE);
                    Map<String, String> customer = read(dbTransaction, "customer", customerIdKey, customerIdValue);
                    if (customer.isEmpty()) {
                        dbService.ic3PieceCommit(dbTransaction, piece1);
                        throw new Exception("customer does not exists");
                    }

//            Update customer
                    lock(dbTransaction, "customer", customerIdKey, customerIdValue, WRITE_TYPE);
                    updateCustomer(warehouseId, districtId, paymentAmount, customerWarehouseId, customerDistrictId, customerId, customer, dbTransaction, customerIdKey, customerIdValue);



                    piece1Res =  dbService.ic3PieceCommit(dbTransaction, piece1);
                    retry++;
                }


                boolean piece2Res = false;
                retry = 0;
                while (!piece2Res) {
                    if (retry > 100) {
                        rollback(dbTransaction);
                        return res;
                    }
                    dbService.ic3PieceBegin(dbTransaction, piece2);


                    //            insert history
                    insertHistory(warehouseId, districtId, paymentAmount, customerWarehouseId, customerDistrictId, customerId, dbTransaction);


                    piece2Res =  dbService.ic3PieceCommit(dbTransaction, piece2);
                    retry++;
                }


                getCommitResult(dbTransaction, res);
            } catch (Exception e) {
                log.error(e.getMessage());
                rollback(dbTransaction);
                return res;
            }
        }

        return res;
    }

    public TransactionResult TPCC_paymentWW(String warehouseId, String districtId, float paymentAmount, String customerWarehouseId, String customerDistrictId, String customerId, String txId) {
        TransactionResult res = new TransactionResult();
        DBTransaction dbTransaction = null;
        try {
            dbTransaction = dbService.getTransaction(Data.newBuilder().setTransactionId(txId).build());
            res.setTxId(dbTransaction.getTimestamp());
        } catch (SQLException e) {
            return res;
        }
        if (dbTransaction != null) {
            try {


//            Get warehouse
                lock(dbTransaction, "warehouse", "w_id", warehouseId, READ_TYPE);
                Map<String, String> warehouse = read(dbTransaction, "warehouse", "w_id", warehouseId);
                if (warehouse.isEmpty()) {
                    log.info("warehouse does not exists");
                    commit(dbTransaction);
                    return res;

                }

//            update warehouse
                lock(dbTransaction, "warehouse", "w_id", warehouseId, WRITE_TYPE);
                float newWarehouseBalance = Float.parseFloat(warehouse.get("w_ytd")) + paymentAmount;
                write(dbTransaction, "warehouse", "w_id", warehouseId, "w_ytd", String.valueOf(newWarehouseBalance)) ;



//          get district
                lock(dbTransaction, "district", "d_w_id,d_id", warehouseId + "," + districtId, READ_TYPE);
                Map<String, String> district = read(dbTransaction, "district", "d_w_id,d_id", warehouseId + "," + districtId);
                if (district.isEmpty())
                    throw new Exception("district does not exists");
                //            Update District
                lock(dbTransaction, "district", "d_w_id,d_id", warehouseId + "," + districtId, WRITE_TYPE);
                float newDistrictBalance = Float.parseFloat(district.get("d_ytd")) + paymentAmount;
                write(dbTransaction, "district", "d_w_id,d_id", warehouseId + "," + districtId, "d_ytd", String.valueOf(newDistrictBalance)) ;

                //            Get customer
                String customerIdKey = "c_w_id,c_d_id,c_id";
                String customerIdValue = customerWarehouseId + "," + customerDistrictId + "," + customerId;
                lock(dbTransaction, "customer", customerIdKey, customerIdValue, READ_TYPE);
                Map<String, String> customer = read(dbTransaction, "customer", customerIdKey, customerIdValue);
                if (customer.isEmpty())
                    throw new Exception("customer does not exists");

//            Update customer
                lock(dbTransaction, "customer", customerIdKey, customerIdValue, WRITE_TYPE);
                updateCustomer(warehouseId, districtId, paymentAmount, customerWarehouseId, customerDistrictId, customerId, customer, dbTransaction, customerIdKey, customerIdValue);

                //            insert history
                insertHistory(warehouseId, districtId, paymentAmount, customerWarehouseId, customerDistrictId, customerId, dbTransaction);


                getCommitResult(dbTransaction, res);
            } catch (Exception e) {
                log.error(e.getMessage());
                rollback(dbTransaction);
                return res;
            }
        }

        return res;
    }

    public TransactionResult TPCC_paymentBamboo(String warehouseId, String districtId, float paymentAmount, String customerWarehouseId, String customerDistrictId, String customerId, String txId) {
        TransactionResult res = new TransactionResult();
        DBTransaction dbTransaction = null;
        try {
            dbTransaction = dbService.getTransaction(Data.newBuilder().setTransactionId(txId).build());
            res.setTxId(dbTransaction.getTimestamp());
        } catch (SQLException e) {
            return res;
        }
        if (dbTransaction != null) {
            try {


//            Get warehouse
                lock(dbTransaction, "warehouse", "w_id", warehouseId, READ_TYPE);
                Map<String, String> warehouse = read(dbTransaction, "warehouse", "w_id", warehouseId);
                if (warehouse.isEmpty()) {
                    log.info("warehouse does not exists");
                    commit(dbTransaction);
                    return res;
                }

//            update warehouse
                lock(dbTransaction, "warehouse", "w_id", warehouseId, WRITE_TYPE);
                float newWarehouseBalance = Float.parseFloat(warehouse.get("w_ytd")) + paymentAmount;
                write(dbTransaction, "warehouse", "w_id", warehouseId, "w_ytd", String.valueOf(newWarehouseBalance)) ;

                retireLock(dbTransaction, "warehouse", "w_id", warehouseId);

//          get district
                lock(dbTransaction, "district", "d_w_id,d_id", warehouseId + "," + districtId, READ_TYPE);
                Map<String, String> district = read(dbTransaction, "district", "d_w_id,d_id", warehouseId + "," + districtId);
                if (district.isEmpty())
                    throw new Exception("district does not exists");
                //            Update District
                lock(dbTransaction, "district", "d_w_id,d_id", warehouseId + "," + districtId, WRITE_TYPE);
                float newDistrictBalance = Float.parseFloat(district.get("d_ytd")) + paymentAmount;
                write(dbTransaction, "district", "d_w_id,d_id", warehouseId + "," + districtId, "d_ytd", String.valueOf(newDistrictBalance)) ;

                retireLock(dbTransaction, "district", "d_w_id,d_id", warehouseId + "," + districtId);

                //            Get customer
                String customerIdKey = "c_w_id,c_d_id,c_id";
                String customerIdValue = customerWarehouseId + "," + customerDistrictId + "," + customerId;
                lock(dbTransaction, "customer", customerIdKey, customerIdValue, READ_TYPE);
                Map<String, String> customer = read(dbTransaction, "customer", customerIdKey, customerIdValue);
                if (customer.isEmpty())
                    throw new Exception("customer does not exists");

//            Update customer
                lock(dbTransaction, "customer", customerIdKey, customerIdValue, WRITE_TYPE);
                updateCustomer(warehouseId, districtId, paymentAmount, customerWarehouseId, customerDistrictId, customerId, customer, dbTransaction, customerIdKey, customerIdValue);

                //            insert history
                insertHistory(warehouseId, districtId, paymentAmount, customerWarehouseId, customerDistrictId, customerId, dbTransaction);

                waitForCommit(dbTransaction);

                getCommitResult(dbTransaction, res);
            } catch (Exception e) {
                log.error(e.getMessage());
                rollback(dbTransaction);
                return res;
            }
        }

        return res;
    }

    public TransactionResult TPCC_paymentSLW(String warehouseId, String districtId, float paymentAmount, String customerWarehouseId, String customerDistrictId, String customerId, String txId) {
        TransactionResult res = new TransactionResult();
        DBTransaction dbTransaction = null;
        try {
            dbTransaction = dbService.getTransaction(Data.newBuilder().setTransactionId(txId).build());
            res.setTxId(dbTransaction.getTimestamp());
        } catch (SQLException e) {
            return res;
        }
        if (dbTransaction != null) {
            try {
//            Early Lock of Customer
                String customerIdKey = "c_w_id,c_d_id,c_id";
                String customerIdValue = customerWarehouseId + "," + customerDistrictId + "," + customerId;
                lock(dbTransaction, "customer", customerIdKey, customerIdValue, WRITE_TYPE);

//            Get warehouse
                lock(dbTransaction, "warehouse", "w_id", warehouseId, WRITE_TYPE);
                Map<String, String> warehouse = read(dbTransaction, "warehouse", "w_id", warehouseId);
                if (warehouse.isEmpty()) {
                    log.info("warehouse does not exists");
                    commit(dbTransaction);
                    return res;
                }

//            update warehouse
                float newWarehouseBalance = Float.parseFloat(warehouse.get("w_ytd")) + paymentAmount;
                write(dbTransaction, "warehouse", "w_id", warehouseId, "w_ytd", String.valueOf(newWarehouseBalance)) ;



//          get district
                lock(dbTransaction, "district", "d_w_id,d_id", warehouseId + "," + districtId, WRITE_TYPE);
                Map<String, String> district = read(dbTransaction, "district", "d_w_id,d_id", warehouseId + "," + districtId);
                if (district.isEmpty())
                    throw new Exception("district does not exists");
                //            Update District
                float newDistrictBalance = Float.parseFloat(district.get("d_ytd")) + paymentAmount;
                write(dbTransaction, "district", "d_w_id,d_id", warehouseId + "," + districtId, "d_ytd", String.valueOf(newDistrictBalance)) ;


//                Early Release of Locks: Unlock warehouse and district
               if (!Performance.INTERACTIVE && Performance.PARTIAL_CHOPPING) {
                    unlock(dbTransaction, "warehouse", "w_id", warehouseId);
                    unlock(dbTransaction, "district", "d_w_id,d_id", warehouseId + "," + districtId);
                }

                //            Get customer
                Map<String, String> customer = read(dbTransaction, "customer", customerIdKey, customerIdValue);
                if (customer.isEmpty())
                    throw new Exception("customer does not exists");

//            Update customer
                updateCustomer(warehouseId, districtId, paymentAmount, customerWarehouseId, customerDistrictId, customerId, customer, dbTransaction, customerIdKey, customerIdValue);

                //            insert history
                insertHistory(warehouseId, districtId, paymentAmount, customerWarehouseId, customerDistrictId, customerId, dbTransaction);


                getCommitResult(dbTransaction, res);
            } catch (Exception e) {
                log.error(e.getMessage());
                rollback(dbTransaction);
                return res;
            }
        }

        return res;
    }

    public TransactionResult TPCC_paymentOrdered(String warehouseId, String districtId, float paymentAmount, String customerWarehouseId, String customerDistrictId, String customerId, String txId) {
        TransactionResult res = new TransactionResult();
        DBTransaction dbTransaction = null;
        try {
            dbTransaction = dbService.getTransaction(Data.newBuilder().setTransactionId(txId).build());
            res.setTxId(dbTransaction.getTimestamp());
        } catch (SQLException e) {
            return res;
        }
        if (dbTransaction != null) {
            try {
                String customerIdKey = "c_w_id,c_d_id,c_id";
                String customerIdValue = customerWarehouseId + "," + customerDistrictId + "," + customerId;

                List<List<String>> allLocks = new ArrayList<>();
                allLocks.add(List.of("warehouse", "w_id", warehouseId, WRITE_TYPE));
                allLocks.add(List.of("district", "d_w_id,d_id", warehouseId + "," + districtId, WRITE_TYPE));
                allLocks.add(List.of("customer", customerIdKey, customerIdValue, WRITE_TYPE));

                allLocks.sort(Comparator.comparing(a -> (a.get(0) + a.get(1) + a.get(2))));
                Collections.reverse(allLocks);

                for (List<String> lockParams : allLocks) {
                    String table = lockParams.get(0);
                    String key = lockParams.get(1);
                    String value = lockParams.get(2);
                    String type = lockParams.get(3);

                    lock(dbTransaction, table, key, value, type);
                }




//            Get warehouse
                Map<String, String> warehouse = read(dbTransaction, "warehouse", "w_id", warehouseId);
                if (warehouse.isEmpty()) {
                    log.info("warehouse does not exists");
                    commit(dbTransaction);
                    return res;

                }

//            update warehouse

                float newWarehouseBalance = Float.parseFloat(warehouse.get("w_ytd")) + paymentAmount;
                write(dbTransaction, "warehouse", "w_id", warehouseId, "w_ytd", String.valueOf(newWarehouseBalance)) ;



//          get district
                Map<String, String> district = read(dbTransaction, "district", "d_w_id,d_id", warehouseId + "," + districtId);
                if (district.isEmpty())
                    throw new Exception("district does not exists");
                //            Update District

                float newDistrictBalance = Float.parseFloat(district.get("d_ytd")) + paymentAmount;
                write(dbTransaction, "district", "d_w_id,d_id", warehouseId + "," + districtId, "d_ytd", String.valueOf(newDistrictBalance)) ;

                //            Get customer


                Map<String, String> customer = read(dbTransaction, "customer", customerIdKey, customerIdValue);
                if (customer.isEmpty())
                    throw new Exception("customer does not exists");

//            Update customer
                updateCustomer(warehouseId, districtId, paymentAmount, customerWarehouseId, customerDistrictId, customerId, customer, dbTransaction, customerIdKey, customerIdValue);

                //            insert history
                insertHistory(warehouseId, districtId, paymentAmount, customerWarehouseId, customerDistrictId, customerId, dbTransaction);


                getCommitResult(dbTransaction, res);
            } catch (Exception e) {
                log.error(e.getMessage());
                rollback(dbTransaction);
                return res;
            }
        }

        return res;
    }

    private void insertHistory(String warehouseId, String districtId, float paymentAmount, String customerWarehouseId, String customerDistrictId, String customerId, DBTransaction tx) throws Exception {
        String historyId = customerId + "," + customerDistrictId + "," + customerWarehouseId + "," + districtId + "," + warehouseId;
//                No need for lock fo insert into history as it does not have a primary key
//                insertLock(tx, "history", historyId);
        String h_data = "'" + warehouseId + ":" + districtId + "'";
        insert(tx, "history", "'" + new Timestamp(System.currentTimeMillis()) + "'" + "," +
                paymentAmount + "," +
                h_data, historyId) ;
    }

    private void updateCustomer(String warehouseId, String districtId, float paymentAmount, String customerWarehouseId, String customerDistrictId, String customerId, Map<String, String> customer, DBTransaction tx, String customerIdKey, String customerIdValue) throws Exception {
        Map<String, String> updateMap = new HashMap<>();
        updateMap.put("c_balance", String.valueOf(Float.parseFloat(customer.get("c_balance")) - paymentAmount));
        updateMap.put("c_ytd_payment", String.valueOf(Float.parseFloat(customer.get("c_ytd_payment")) + paymentAmount));
        updateMap.put("c_payment_cnt", String.valueOf(Integer.parseInt(customer.get("c_payment_cnt")) + 1));
        if (customer.get("c_credit").equals("BC")) {
            String c_data = customerId + " " + customerDistrictId + " " + customerWarehouseId + " " + districtId + " " + warehouseId + " " + paymentAmount + " | " + customer.get("c_data");
            if (c_data.length() > 500) {
                c_data = c_data.substring(0, 500);
            }
            updateMap.put("c_data", c_data);


        }
        write(tx, "customer", customerIdKey, customerIdValue, updateMap);
    }


    public TransactionResult interactiveTransaction(List<List<String>> modifyLists, String txId) {
        TransactionResult res = new TransactionResult();
        DBTransaction dbTransaction = null;
        try {
            dbTransaction = dbService.getTransaction(
                    Data.newBuilder().setTransactionId(txId).setInteractiveTransaction(true).build());
            res.setTxId(dbTransaction.getTimestamp());
        } catch (SQLException e) {
            return res;
        }
        if (dbTransaction != null) {
            try {
                for (int i = 0; i < modifyLists.size(); i++) {
                    List<String> modifyList = modifyLists.get(i);
                    String table = modifyList.get(0);
                    String key = modifyList.get(1);
                    String value = modifyList.get(2);

                    lock(dbTransaction, table, key, value, READ_TYPE);
                    Map<String, String> record = read(dbTransaction, table, key, value);

                    lock(dbTransaction, table, key, value, WRITE_TYPE);
                    write(dbTransaction, table, key, value, record);

                    if (Objects.equals(mode, "bamboo") && lastWrite(i, modifyLists)) {
                        retireLock(dbTransaction, table, key, value);
                    }
                }

                if (mode.equals("bamboo"))
                    waitForCommit(dbTransaction);
                getCommitResult(dbTransaction, res);
            } catch (Exception e) {
                log.error(e.getMessage());
                rollback(dbTransaction);
                return res;
            }
        }
        return res;
    }

    private boolean lastWrite(int i, List<List<String>> modifyLists) {
        for (int j = i+1; j < modifyLists.size(); j++) {
            if (new HashSet<String>(modifyLists.get(i)).equals(new HashSet<String>(modifyLists.get(j))))
                return false;
        }
        return true;
    }


    public TransactionResult TPCC_orderStatus(String warehouseId, String districtId, String customerId, String txId) {
        TransactionResult res = new TransactionResult();
        DBTransaction dbTransaction = null;
        try {
            dbTransaction = dbService.getTransaction(
                    Data.newBuilder().setTransactionId(txId).build());
            res.setTxId(dbTransaction.getTimestamp());
        } catch (SQLException e) {
            return res;
        }
        if (dbTransaction != null) {
            try {
                // Get customer
                String customerKey = "c_w_id,c_d_id,c_id";
                String customerValue = warehouseId + "," + districtId + "," + customerId;
                lock(dbTransaction, "customer", customerKey, customerValue, READ_TYPE);
                Map<String, String> customer = read(dbTransaction, "customer", customerKey, customerValue);
                if (customer.isEmpty())
                    throw new Exception("customer does not exist");

                // Get latest open order for this customer.
                // (Assuming an index or composite key exists on open orders by customer.)
                String oorderKey = "o_w_id,o_d_id,o_c_id";
                String oorderValue = warehouseId + "," + districtId + "," + customerId;
                lock(dbTransaction, "oorder", oorderKey, oorderValue, READ_TYPE);
                Map<String, String> order = read(dbTransaction, "oorder", oorderKey, oorderValue);
                // If no open order exists, then simply commit.
                if (order.isEmpty()) {
//                    getCommitResult(dbTransaction, res);
//                    return res;
                    order = Map.of("o_id", String.valueOf(random.nextInt(10000))); // dummy order
                }

                // Get order lines for the retrieved order.
                String orderId = order.get("o_id");
                String orderLineKey = "ol_w_id,ol_d_id,ol_o_id";
                String orderLineValue = warehouseId + "," + districtId + "," + orderId;
                lock(dbTransaction, "order_line", orderLineKey, orderLineValue, READ_TYPE);
                Map<String, String> orderLines = read(dbTransaction, "order_line", orderLineKey, orderLineValue);
                // (You might aggregate or log the order-lines as needed.)

                getCommitResult(dbTransaction, res);
            } catch (Exception e) {
                log.error(e.getMessage());
                rollback(dbTransaction);
                return res;
            }
        }
        return res;
    }

//    public TransactionResult TPCC_deliveryWW(String warehouseId, String o_carrier_id, String txId) {
//        TransactionResult res = new TransactionResult();
//        DBTransaction dbTransaction = null;
//        try {
//            dbTransaction = dbService.getTransaction(
//                    Data.newBuilder().setTransactionId(txId).build());
//            res.setTxId(dbTransaction.getTimestamp());
//        } catch (SQLException e) {
//            return res;
//        }
//        // Assume there are 10 districts per warehouse.
//        int districtCount = 10;
//        if (dbTransaction != null) {
//            try {
//                for (int d = 1; d <= districtCount; d++) {
//                    String districtId = String.valueOf(d);
//
//                    // Get the lowest new order id for district.
//                    String newOrderKey = "no_w_id,no_d_id";
//                    String newOrderValue = warehouseId + "," + districtId;
//                    lock(dbTransaction, "new_order", newOrderKey, newOrderValue, READ_TYPE);
//                    // Assume scanNewOrder returns the lowest order id (or empty if none)
//                    String no_o_id = scanNewOrder(dbTransaction, warehouseId, districtId);
//                    if (no_o_id == null) {
//                        continue; // no new order in this district
//                    }
//
//                    // Delete the new order record.
//                    String newOrderFullKey = warehouseId + "," + districtId + "," + no_o_id;
//                    delete(dbTransaction, "new_order", newOrderFullKey);
//
//                    // Get the corresponding open order to obtain customer id.
//                    String oorderKey = "o_w_id,o_d_id,o_id";
//                    String oorderValue = warehouseId + "," + districtId + "," + no_o_id;
//                    lock(dbTransaction, "oorder", oorderKey, oorderValue, READ_TYPE);
//                    Map<String, String> openOrder = read(dbTransaction, "oorder", oorderKey, oorderValue);
//                    if (openOrder.isEmpty())
//                        throw new Exception("open order not found for key " + oorderValue);
//                    String customerId = openOrder.get("o_c_id");
//
//                    // Update open order: set the carrier id.
//                    lock(dbTransaction, "oorder", oorderKey, oorderValue, WRITE_TYPE);
//                    write(dbTransaction, "oorder", oorderKey, oorderValue, "o_carrier_id", o_carrier_id);
//
//                    // Update order lines: set delivery date to current timestamp.
//                    String orderLineKey = "ol_w_id,ol_d_id,ol_o_id";
//                    String orderLineValue = warehouseId + "," + districtId + "," + no_o_id;
//                    lock(dbTransaction, "order_line", orderLineKey, orderLineValue, WRITE_TYPE);
//                    String deliveryTimestamp = "'" + new Timestamp(System.currentTimeMillis()) + "'";
//                    // Assume updateOrderLineDelivery updates all order_line rows for the order.
//                    updateOrderLineDelivery(dbTransaction, orderLineValue, deliveryTimestamp);
//
//                    // Sum the order-line amounts (assume we can read an aggregated value).
//                    lock(dbTransaction, "order_line", orderLineKey, orderLineValue, READ_TYPE);
//                    Map<String, String> orderLineAgg = read(dbTransaction, "order_line", orderLineKey, orderLineValue);
//                    // Assume the aggregated result returns "ol_total"
//                    float ol_total = Float.parseFloat(orderLineAgg.get("ol_total"));
//
//                    // Update customer: add order total to balance and increment delivery count.
//                    String customerKey = "c_w_id,c_d_id,c_id";
//                    String customerValue = warehouseId + "," + districtId + "," + customerId;
//                    lock(dbTransaction, "customer", customerKey, customerValue, READ_TYPE);
//                    Map<String, String> customer = read(dbTransaction, "customer", customerKey, customerValue);
//                    lock(dbTransaction, "customer", customerKey, customerValue, WRITE_TYPE);
//                    float newBalance = Float.parseFloat(customer.get("c_balance")) + ol_total;
//                    int newDeliveryCnt = Integer.parseInt(customer.get("c_delivery_cnt")) + 1;
//                    Map<String, String> custUpdates = new HashMap<>();
//                    custUpdates.put("c_balance", String.valueOf(newBalance));
//                    custUpdates.put("c_delivery_cnt", String.valueOf(newDeliveryCnt));
//                    write(dbTransaction, "customer", customerKey, customerValue, custUpdates);
//                }
//                getCommitResult(dbTransaction, res);
//            } catch (Exception e) {
//                log.error(e.getMessage());
//                rollback(dbTransaction);
//                return res;
//            }
//        }
//        return res;
//    }

//    TODO: also add stock level if needed
//    public TransactionResult TPCC_stockLevel(String warehouseId, String districtId, String threshold, String txId) {
//        TransactionResult res = new TransactionResult();
//        DBTransaction dbTransaction = null;
//        try {
//            dbTransaction = dbService.getTransaction(
//                    Data.newBuilder().setTransactionId(txId).build());
//            res.setTxId(dbTransaction.getTimestamp());
//        } catch (SQLException e) {
//            return res;
//        }
//        if (dbTransaction != null) {
//            try {
//                // Get district to retrieve d_next_o_id
//                String districtKey = "d_w_id,d_id";
//                String districtValue = warehouseId + "," + districtId;
//                lock(dbTransaction, "district", districtKey, districtValue, READ_TYPE);
//                Map<String, String> district = read(dbTransaction, "district", districtKey, districtValue);
//                if (district.isEmpty())
//                    throw new Exception("district does not exist");
//                String d_next_o_id = district.get("d_next_o_id");
//
//                // Calculate the lower bound for order ids (d_next_o_id - 20)
//                int lowerOrderId = Integer.parseInt(d_next_o_id) - 20;
//
//                // Execute a join query on order_line and stock to count distinct stock items with quantity below the threshold.
//                // Here we simulate this via a single read with a composite key.
//                String joinKey = "ol_w_id,ol_d_id,ol_range,threshold";
//                String joinValue = warehouseId + "," + districtId + "," + lowerOrderId + "," + d_next_o_id + "," + threshold;
//                lock(dbTransaction, "order_line_stock", joinKey, joinValue, READ_TYPE);
//                Map<String, String> stockLevelResult = read(dbTransaction, "order_line_stock", joinKey, joinValue);
//                // Assume the result contains "stock_count"
//                String stock_count = stockLevelResult.get("stock_count");
//
//                // You could log or include the stock_count in the result.
//                getCommitResult(dbTransaction, res);
//            } catch (Exception e) {
//                log.error(e.getMessage());
//                rollback(dbTransaction);
//                return res;
//            }
//        }
//        return res;
//    }




    public void setServerConfig(Map<String, String> config) {
        Result result = dbService.setConfig(Config.newBuilder().putAllValues(config).build());
        if (!result.getStatus())
            throw new RuntimeException("Could not set the server config!");

    }

    public boolean readItem(List<String> items) {
        DBTransaction dbTransaction = null;
        try {
            dbTransaction = dbService.getTransaction(Data.newBuilder().setTransactionId("").build());
        } catch (SQLException e) {
            return false;
        }
        if (dbTransaction != null) {

            if (mode.equals("bamboo")) {
                if (readItemBamboo(items, dbTransaction)) return false;
            } else if (mode.equals("ww") || mode.equals("occ")) {
                if (readItems(items, dbTransaction)) return false;
            } else if (mode.equals("ordered")) {
                if (readItemsOrdered(items, dbTransaction)) return false;
            } else if (mode.equals("ic3")) {
                if (readItemsIC3(items, dbTransaction)) return false;
            }
            else {
                if (readItemSLW(items, dbTransaction)) return false;
            }
            return commit(dbTransaction).getStatus();

        }
        return false;
    }

    private boolean readItemsIC3(List<String> items, DBTransaction dbTransaction) {
        IC3Piece piece1 = new IC3Piece(Set.of(
                "buyListing:p1",
                "addListing:p1"
        ), "readItems:p1", dbTransaction.getTimestampValue());
        dbTransaction.addIC3Piece(piece1);


        try {

            boolean piece1Res = false;
            int retry = 0;
            while (!piece1Res) {
                if (retry > 100) {
                    rollback(dbTransaction);
                    return true;
                }
                dbService.ic3PieceBegin(dbTransaction, piece1);

                try {
                    for (String IId :
                            items) {
                        lock(dbTransaction, "Items", "IId", IId, READ_TYPE);
                        Map<String, String> item = read(dbTransaction, "Items", "IId", IId);

                    }
                } catch (Exception e) {
                    rollback(dbTransaction);
                    return true;
                }


                piece1Res = dbService.ic3PieceCommit(dbTransaction, piece1);
                retry++;
            }
        }
        catch (Exception e) {
            return true;
        }


       return false;
    }

    private boolean readItemsOrdered(List<String> items, DBTransaction dbTransaction) {
                Collections.sort(items);
        return readItems(items, dbTransaction);
    }

    private boolean readItemSLW(List<String> items, DBTransaction tx) {
//        Collections.sort(items);
        return readItems(items, tx);
    }

    private boolean readItems(List<String> items, DBTransaction tx) {
        try {
            for (String IId :
                    items) {
                lock(tx, "Items", "IId", IId, READ_TYPE);
                Map<String, String> item = read(tx, "Items", "IId", IId);

//                if (mode.equals("bamboo"))
//                    retireLock(tx, "Items", "IId", IId);
//                else
                if (mode.equals("slw"))
                    unlock(tx, "Items", "IId", IId);
//                else if ()

            }
        } catch (Exception e) {
            rollback(tx);
            return true;
        }
        return false;
    }

    private boolean readItemBamboo(List<String> items, DBTransaction tx) {
        if (readItems(items, tx)) return true;
        try {
            waitForCommit(tx);
        } catch (Exception e) {
            rollback(tx);
            return true;
        }
        return false;
    }

//    private void simulateDeadlock() {
//        String tx1 = dbService.beginTransaction(Data.newBuilder().build()).getMessage();
//        String tx2 = dbService.beginTransaction(Data.newBuilder().build()).getMessage();
//
//        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(5);
//        executor.submit(() -> lock(tx1, "Players", "PId", "10"));
//        delay();
//        executor.submit(() -> lock(tx2, "Players", "PId", "11"));
//        delay();
//        executor.submit(() -> {
//            try {
//                lock(tx1, "Players", "PId", "11");
//            } catch (Exception e) {
//                rollback(tx1);
//            }
//        });
//        delay();
//        executor.submit(() -> {
//
//            try {
//                lock(tx2, "Players", "PId", "10");
//            } catch (Exception e) {
//                rollback(tx2);
//            }
//        });// DEADLOCK
//
//
//        while (executor.getActiveCount() != 0) {
//            delay();
//        }
//
//
//        commit(tx1);
//        commit(tx2);
//
//
//    }

    private static void delay() {
        delay(100);
    }

    private static void delay(long mili) {
        try {
            Thread.sleep(mili);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }


//    remoteCalls : 4

    public TransactionResult addListing(String PId, String IId, double price, String txId) {
        return switch (mode) {
            case "slw" -> addListingSLW(PId, IId, price, txId);
            case "ordered" -> addListingOrdered(PId, IId, price, txId);
            case "bamboo" -> addListingBamboo(PId, IId, price, txId);
            case "ww", "occ"-> addListingWW(PId, IId, price, txId);
            case "ic3" -> addListingIC3(PId, IId, price, txId);
//            case "occ" -> dummyOCC();
            default -> new TransactionResult();
        };
    }

    public TransactionResult dummyOCC() {
        TransactionResult res = new TransactionResult();
        DBTransaction dbTransaction = null;
        try {
            dbTransaction = dbService.getTransaction(Data.newBuilder().build());
            res.setTxId(dbTransaction.getTimestamp());
        } catch (SQLException e) {
            return res;
        }
        if (dbTransaction != null) {
            try {
                Map<String, String> x = read(dbTransaction, "Items", "IId","1000");
                write(dbTransaction, "Items", "IId", "1000", x);
                getCommitResult(dbTransaction, res);
                res.setMessage("1");
                return res;
            } catch (Exception e) {
                rollback(dbTransaction);
                return res;
            }
        }

        return res;
    }


    public TransactionResult addListingIC3(String PId, String IId, double price, String txId) {

        TransactionResult res = new TransactionResult();
        DBTransaction dbTransaction = null;
        try {
            dbTransaction = dbService.getTransaction(Data.newBuilder().setTransactionId(txId).build());
            res.setTxId(dbTransaction.getTimestamp());
        } catch (SQLException e) {
            return res;
        }
        if (dbTransaction != null) {
            try {
//
//            Map<String, String> listing = read(dbTransaction, "Listings", "LIId", IId);
////            Check no listing exists with the item id
//            if (!listing.isEmpty()) {
//                log.info("listing exists");
//                commit(dbTransaction);
//                return;
//            }

//                Map<String, String> x = read(dbTransaction, "Items", "IId","1000");
//                write(dbTransaction, "Items", "IId", "1000", x);

                IC3Piece piece1 = new IC3Piece(Set.of(
                        "buyListing:p1",
                        "addListing:p1"
                ), "addListing:p1", dbTransaction.getTimestampValue());
                dbTransaction.addIC3Piece(piece1);




                String listingRecordId = null;

                boolean piece1Res = false;
                int retry = 0;
                long wastedTime = 0;
                long startTime = System.nanoTime();
                while (!piece1Res) {
                    wastedTime += System.nanoTime() - startTime;
                    startTime = System.nanoTime();
                    dbTransaction.resetTimes();


                    if (retry > 100) {
                        rollback(dbTransaction);
                        return res;
                    }
                    dbService.ic3PieceBegin(dbTransaction, piece1);
                    Map<String, String> item = read(dbTransaction, "Items", "IId", IId);

                    if (item.isEmpty()) {
                        log.info("Item does not exists!");
                        dbService.ic3PieceCommit(dbTransaction, piece1);
                        commit(dbTransaction);
                        return res;
                    }

                    if (Integer.parseInt(item.get("IOwner")) != Integer.parseInt(PId)) {
                        log.info("item has a different owner! {}<>{}", item.get("IOwner"), PId);
                        dbService.ic3PieceCommit(dbTransaction, piece1);
                        commit(dbTransaction);
                        return res;
                    }

                    Map<String, String> player = read(dbTransaction, "Players", "PId", PId);
                    //            Check player exists
                    if (player.isEmpty()) {
                        log.info("player does not exists!");
                        commit(dbTransaction);
                        return res;
                    }

                    Result insertListingResult = insertLock(dbTransaction, "Listings");
                    listingRecordId = insertListingResult.getMessage();
                    insert(dbTransaction, "Listings", IId + "," + price, listingRecordId, "LIId,LPrice");


                    piece1Res = dbService.ic3PieceCommit(dbTransaction, piece1);
                    retry++;
                }


                getCommitResult(dbTransaction, res);
                if (res.metrics != null) {
                    HashMap<String, String> newMetrics = new HashMap<>(res.metrics);
                    newMetrics.put("wasted", String.valueOf(wastedTime));
                    newMetrics.put("retry", String.valueOf(retry - 1));
                    res.setMetrics(newMetrics);
                }

                res.setMessage(listingRecordId);
                return res;
            } catch (Exception e) {
                rollback(dbTransaction);
                return res;
            }
        }

        return res;
    }

    public TransactionResult addListingWW(String PId, String IId, double price, String txId) {

        TransactionResult res = new TransactionResult();
        DBTransaction dbTransaction = null;
        try {
            dbTransaction = dbService.getTransaction(Data.newBuilder().setTransactionId(txId).build());
            res.setTxId(dbTransaction.getTimestamp());
        } catch (SQLException e) {
            return res;
        }
        if (dbTransaction != null) {
            try {
//
//            Map<String, String> listing = read(dbTransaction, "Listings", "LIId", IId);
////            Check no listing exists with the item id
//            if (!listing.isEmpty()) {
//                log.info("listing exists");
//                commit(dbTransaction);
//                return;
//            }

//                Map<String, String> x = read(dbTransaction, "Items", "IId","1000");
//                write(dbTransaction, "Items", "IId", "1000", x);

                lock(dbTransaction, "Items", "IId", IId, READ_TYPE);
                Map<String, String> item = read(dbTransaction, "Items", "IId", IId);
                if (item.isEmpty()) {
                    log.info("Item does not exists!");
                    commit(dbTransaction);
                    return res;
                }
//             Check the owner
                if (Integer.parseInt(item.get("IOwner")) != Integer.parseInt(PId)) {
                    log.info("item has a different owner! {}<>{}", item.get("IOwner"), PId);
                    commit(dbTransaction);
                    return res;
                }

                lock(dbTransaction, "Players", "PId", PId, READ_TYPE);
                Map<String, String> player = read(dbTransaction, "Players", "PId", PId);
//            Check player exists
                if (player.isEmpty()) {
                    log.info("player does not exists!");
                    commit(dbTransaction);
                    return res;
                }

                Result insertListingResult = insertLock(dbTransaction, "Listings");
                String listingRecordId = insertListingResult.getMessage();
                insert(dbTransaction, "Listings", IId + "," + price, listingRecordId, "LIId,LPrice");
                getCommitResult(dbTransaction, res);
                res.setMessage(listingRecordId);
                return res;
            } catch (Exception e) {
                rollback(dbTransaction);
                return res;
            }
        }

        return res;
    }


    //    remoteCalls : 4
    public TransactionResult addListingBamboo(String PId, String IId, double price, String txId) {

        TransactionResult res = new TransactionResult();
        DBTransaction dbTransaction = null;
        try {
            dbTransaction = dbService.getTransaction(Data.newBuilder().setTransactionId(txId).build());
            res.setTxId(dbTransaction.getTimestamp());
        } catch (SQLException e) {
            return res;
        }
        if (dbTransaction != null) {
            try {
//
//            Map<String, String> listing = read(transactionId, "Listings", "LIId", IId);
////            Check no listing exists with the item id
//            if (!listing.isEmpty()) {
//                log.info("listing exists");
//                commit(transactionId);
//                return;
//            }

                lock(dbTransaction, "Items", "IId", IId, READ_TYPE);
                Map<String, String> item = read(dbTransaction, "Items", "IId", IId);

                if (item.isEmpty()) {
                    log.info("Item does not exists!");
                    commit(dbTransaction);
                    return res;
                }
//             Check the owner
                if (Integer.parseInt(item.get("IOwner")) != Integer.parseInt(PId)) {
                    log.info("item has a different owner! {}<>{}", item.get("IOwner"), PId);
                    commit(dbTransaction);
                    return res;
                }


                lock(dbTransaction, "Players", "PId", PId, READ_TYPE);
                Map<String, String> player = read(dbTransaction, "Players", "PId", PId);

//            Check player exists
                if (player.isEmpty()) {
                    log.info("player does not exists!");
                    commit(dbTransaction);
                    return res;
                }

                Result insertListingResult = insertLock(dbTransaction, "Listings");
                String listingRecordId = insertListingResult.getMessage();
                insert(dbTransaction, "Listings", IId + "," + price, listingRecordId, "LIId,LPrice");
//
                waitForCommit(dbTransaction);

                getCommitResult(dbTransaction, res);
                res.setMessage(listingRecordId);
                return res;
            } catch (Exception e) {
                rollback(dbTransaction);
                return res;
            }
        }

        return res;
    }



    public TransactionResult buyListingBamboo(String PId, String LId, String txId) {
        TransactionResult res = new TransactionResult();
        DBTransaction dbTransaction = null;
        try {
            dbTransaction = dbService.getTransaction(Data.newBuilder().setTransactionId(txId).build());
            res.setTxId(dbTransaction.getTimestamp());
        } catch (SQLException e) {
            return res;
        }
        if (dbTransaction != null) {
            try {

                //            R from L where Lid
                lock(dbTransaction, "Listings", "LId", LId, READ_TYPE);
                Map<String, String> listing = read(dbTransaction, "Listings", "LId", LId);
                retireLock(dbTransaction, "Listings", "LId", LId);
//            Check player exists
                if (listing.isEmpty()) {
                    log.info("listing does not exists!");
                    commit(dbTransaction);
                    return res;
                }

                //            Read from P where pid
                lock(dbTransaction, "Players", "PId", PId, READ_TYPE);
//            TODO: WE MIGHT NEED TO ROLLBACK
                Map<String, String> player = read(dbTransaction, "Players", "PId", PId);
//                retireLock(dbTransaction, "Players", "PId", PId);

//            Check player exists
                if (player.isEmpty()) {
                    log.info("player does not exists!");
                    commit(dbTransaction);
                    return res;
                }

                //            Check players cash for listing
                if (Double.parseDouble(player.get("PCash")) < Double.parseDouble(listing.get("LPrice"))) {
                    log.info("player does not have enough cash");
                    commit(dbTransaction);
                    return res;
                }


//            Read from I where LIId
                lock(dbTransaction, "Items", "IId", listing.get("LIId"), READ_TYPE);
                Map<String, String> item = read(dbTransaction, "Items", "IId", listing.get("LIId"));
//                retireLock(dbTransaction, "Items", "IId", listing.get("LIId"));
                //            Check item exists
                if (item.isEmpty()) {
                    log.info("item does not exists!");
                    commit(dbTransaction);
                    return res;
                }


//            Delete from L where LID
                lock(dbTransaction, "Listings", "LId", LId, WRITE_TYPE);
                delete(dbTransaction, "Listings", "LId", LId);
                retireLock(dbTransaction, "Listings", "LId", LId);


//            R from P where ppid
                String PPId = item.get("IOwner");
                lock(dbTransaction, "Players", "PId", PPId, READ_TYPE);
                Map<String, String> prevOwner = read(dbTransaction, "Players", "PId", PPId);
                //            Check prevOwner exists
                if (prevOwner.isEmpty()) {
                    log.info("previous owner does not exists!");
                    commit(dbTransaction);
                    return res;
                }

//          W into I where IId = LIId SET IOwner = pid
                lock(dbTransaction, "Items", "IId", listing.get("LIId"), WRITE_TYPE);
                write(dbTransaction, "Items", "IId", listing.get("LIId"), "IOwner", PId);


                retireLock(dbTransaction, "Items", "IId", listing.get("LIId"));

//           W into P where Pid SET PCash = new Cash
                lock(dbTransaction, "Players", "PId", PId, WRITE_TYPE);
                String newCash = Double.toString(Double.parseDouble(player.get("PCash")) - Double.parseDouble(listing.get("LPrice")));
                write(dbTransaction, "Players", "PId", PId, "PCash", newCash);

//           W into P where Pid SET PCash = new Cash
                lock(dbTransaction, "Players", "PId", PPId, WRITE_TYPE);
                String prevOwnerNewCash = Double.toString(Double.parseDouble(prevOwner.get("PCash")) + Double.parseDouble(listing.get("LPrice")));
                write(dbTransaction, "Players", "PId", PPId, "PCash", prevOwnerNewCash);


                waitForCommit(dbTransaction);

//            Unlock
                getCommitResult(dbTransaction, res);
                res.setMessage(listing.get("LIId"));
                return res;
            } catch (Exception e) {
                rollback(dbTransaction);
                return res;
            }
        }
        return res;
    }
    private Result waitForCommit(DBTransaction dbTransaction) throws Exception {
        Result result = dbService.bambooWaitForCommit(dbTransaction);
        log.info("{}, waitForCommit status: {} - message: {}",dbTransaction.getTimestamp(), result.getStatus(), result.getMessage());
        if (!result.getStatus())
            throw new Exception(result.getMessage());
        return result;
    }



    private Result retireLock(DBTransaction tx, String tableName, String key, String value) throws Exception {
        Data lockData = Data.newBuilder()
                .setTransactionId(tx.getTimestamp())
                .setType(WRITE_TYPE)
                .setKey(tableName + "," + key + "," + value)
                .build();
        Result retireResult = dbService.bambooRetireLock(lockData, tx);
        log.info("{}, retire the lock {},{}:{} status: {} - message: {}",tx.getTimestamp(), tableName, key, value, retireResult.getStatus(), retireResult.getMessage());
        if (!retireResult.getStatus())
            throw new Exception(retireResult.getMessage());
        return retireResult;
    }



    private void rollback(DBTransaction tx) {
        Result rollbackRes = dbService.rollBackTransaction(tx);
    }


    //    Remote calls: 4
    public TransactionResult addListingSLW(String PId, String IId, double price, String txId) {
        log.info("add listing <PID:{}, IID:{}>", PId, IId);
        TransactionResult res = new TransactionResult();
        DBTransaction dbTransaction = null;
        try {
            dbTransaction = dbService.getTransaction(Data.newBuilder().setTransactionId(txId).build());
            res.setTxId(dbTransaction.getTimestamp());
        } catch (SQLException e) {
            return res;
        }
        if (dbTransaction != null) {
//            String transactionId = dbTransaction.getTimestamp();
            try {
                String listingRecordId = insertLock(dbTransaction, "Listings").getMessage();

                lock(dbTransaction, "Items", "IId", IId, READ_TYPE);
                Map<String, String> item = read(dbTransaction, "Items", "IId", IId);
//             Check the owner
                if (Integer.parseInt(item.get("IOwner")) != Integer.parseInt(PId)) {
                    log.error("item has a different owner! {}<>{}", item.get("IOwner"), PId);
                    commit(dbTransaction);
                    return res;
                }

                atomicLock(dbTransaction, "Players", "PId", List.of(PId));
                if (!Performance.INTERACTIVE && Performance.PARTIAL_CHOPPING)
                    unlock(dbTransaction, "Items", "IId", IId);
                Map<String, String> player = read(dbTransaction, "Players", "PId", PId);
//            Check player exists
                if (player.isEmpty()) {
                    log.error("player does not exists!");
                    commit(dbTransaction);
                    return res;
                }

                insert(dbTransaction, "Listings", IId + "," + price, listingRecordId, "LIId,LPrice");
                getCommitResult(dbTransaction, res);
                res.setMessage(listingRecordId);
                return res;

            } catch (Exception e) {
                log.error(e.getMessage());
                rollback(dbTransaction);
                return res;
            }
        }
        return res;
    }

    public TransactionResult addListingOrdered(String PId, String IId, double price, String txId) {
        log.info("add listing <PID:{}, IID:{}>", PId, IId);
        TransactionResult res = new TransactionResult();
        DBTransaction dbTransaction = null;
        try {
            dbTransaction = dbService.getTransaction(Data.newBuilder().setTransactionId(txId).build());
            res.setTxId(dbTransaction.getTimestamp());
        } catch (SQLException e) {
            return res;
        }
        if (dbTransaction != null) {
//            String transactionId = dbTransaction.getTimestamp();
            try {
                String listingRecordId = "";

                List<List<String>> allLocks =  List.of(
                        List.of("Items", "IId", IId, READ_TYPE),
                        List.of("Listings", "LId", "", INSERT_TYPE),
                        List.of("Players", "PId", PId, READ_TYPE)
                );
                for (List<String> toLock :
                        allLocks) {
                    if (Objects.equals(toLock.get(3), INSERT_TYPE))
                        listingRecordId = insertLock(dbTransaction, toLock.get(0)).getMessage();
                    else
                        lock(dbTransaction, toLock.get(0), toLock.get(1), toLock.get(2), toLock.get(3));
                }

                Map<String, String> item = read(dbTransaction, "Items", "IId", IId);
//             Check the owner
                if (Integer.parseInt(item.get("IOwner")) != Integer.parseInt(PId)) {
                    log.error("item has a different owner! {}<>{}", item.get("IOwner"), PId);
                    commit(dbTransaction);
                    return res;
                }

//                if (!Performance.INTERACTIVE && Performance.PARTIAL_CHOPPING)
//                    unlock(dbTransaction, "Items", "IId", IId);
                Map<String, String> player = read(dbTransaction, "Players", "PId", PId);
//            Check player exists
                if (player.isEmpty()) {
                    log.error("player does not exists!");
                    commit(dbTransaction);
                    return res;
                }

                insert(dbTransaction, "Listings", IId + "," + price, listingRecordId, "LIId,LPrice");
                getCommitResult(dbTransaction, res);
                res.setMessage(listingRecordId);
                return res;

            } catch (Exception e) {
                log.error(e.getMessage());
                rollback(dbTransaction);
                return res;
            }
        }
        return res;
    }

    public TransactionResult lockOrderProblem(String PId, String PPId, String txId) {
        return switch (mode) {
            case "slw" -> lockOrderProblemSLW(PId, PPId, txId);
            case  "ww" -> lockOrderProblemWW(PId, PPId, txId);
            default -> new TransactionResult();
        };
    }
    public TransactionResult lockOrderProblemSLW(String PId, String PPId, String txId) {
        TransactionResult res = new TransactionResult();
        DBTransaction dbTransaction = null;
        try {
            dbTransaction = dbService.getTransaction(Data.newBuilder().setTransactionId(txId).build());
            res.setTxId(dbTransaction.getTimestamp());
        } catch (SQLException e) {
            return res;
        }

        if (dbTransaction != null) {
            try {

// Lock Try
//                lock(dbTransaction, "Players", "PId", PId);
//                if (Integer.parseInt(PPId) > Integer.parseInt(PId)) {
//                    if (!tryLock(dbTransaction, "Players", "PId", PPId, WRITE_TYPE).getStatus()) {
//                        unlock(dbTransaction, "Players", "PId", PId);
//                        lock(dbTransaction, "Players", "PId", PPId);
//                        lock(dbTransaction, "Players", "PId", PId);
//                    }
//                }
//                else {
//                    lock(dbTransaction, "Players", "PId", PPId);
//                }

// atomic lock
                atomicLock(dbTransaction, "Players", "PId", List.of(PId, PPId));


//            TODO: WE MIGHT NEED TO ROLLBACK
                Map<String, String> player = read(dbTransaction, "Players", "PId", PId);
                Map<String, String> player2 = read(dbTransaction, "Players", "PId", PPId);

                getCommitResult(dbTransaction, res);
                this.pidAccessed.computeIfPresent(PId , (key, value) -> value + 1);
                this.pidAccessed.computeIfPresent(PPId, (key, value) -> value + 1);
                this.pidAccessed.putIfAbsent(PId, 1L);
                this.pidAccessed.putIfAbsent( PPId, 1L);

                return res;
            } catch (Exception e) {
                rollback(dbTransaction);
                return res;
            }
        }
        return res;

    }

    public TransactionResult lockOrderProblemWW(String PId, String PPId, String txId) {
        TransactionResult res = new TransactionResult();
        DBTransaction dbTransaction = null;
        try {
            dbTransaction = dbService.getTransaction(Data.newBuilder().setTransactionId(txId).build());
            res.setTxId(dbTransaction.getTimestamp());
        } catch (SQLException e) {
            return res;
        }

        if (dbTransaction != null) {
            try {
                lock(dbTransaction, "Players", "PId", PId);
                lock(dbTransaction, "Players", "PId", PPId);



//            TODO: WE MIGHT NEED TO ROLLBACK
                Map<String, String> player = read(dbTransaction, "Players", "PId", PId);
                Map<String, String> player2 = read(dbTransaction, "Players", "PId", PPId);

                getCommitResult(dbTransaction, res);
                this.pidAccessed.computeIfPresent(PId , (key, value) -> value + 1);
                this.pidAccessed.computeIfPresent(PPId, (key, value) -> value + 1);
                this.pidAccessed.putIfAbsent(PId, 1L);
                this.pidAccessed.putIfAbsent( PPId, 1L);

                return res;
            } catch (Exception e) {
                rollback(dbTransaction);
                return res;
            }
        }
        return res;

    }
    public TransactionResult buyListing(String PId, String LId, String txId) {
        return switch (mode) {
            case "slw" -> buyListingSLW(PId, LId, txId);
            case "ordered" -> buyListingORDERED(PId, LId, txId);
            case "ic3" -> buyListingIC3(PId, LId, txId);
            case "bamboo" -> buyListingBamboo(PId, LId, txId);
            case "ww", "occ"-> buyListingWW(PId, LId, txId);
//            case "occ" -> dummyOCC();
            default -> new TransactionResult();
        };
    }


    public TransactionResult buyListingIC3(String PId, String LId, String txId) {

        TransactionResult res = new TransactionResult();
        DBTransaction dbTransaction = null;
        try {
            dbTransaction = dbService.getTransaction(Data.newBuilder().setTransactionId(txId).build());
            res.setTxId(dbTransaction.getTimestamp());
        } catch (SQLException e) {
            return res;
        }

        if (dbTransaction != null) {
            try {

                IC3Piece piece1 = new IC3Piece(Set.of(
                        "addListing:p1",
                        "buyListing:p1"
                ), "buyListing:p1", dbTransaction.getTimestampValue());
                dbTransaction.addIC3Piece(piece1);
//                TODO: implement grpc call

                boolean p1Result = false;
                int retry = 0;
                long wastedTime = 0;
                long startTime = System.nanoTime();
                while (!p1Result) {
                    wastedTime += System.nanoTime() - startTime;
                    startTime = System.nanoTime();
                    dbTransaction.resetTimes();
                    if (retry > 100)
                    {
                        rollback(dbTransaction);
                        return res;
                    }
                    p1Result = runBuyListingIC3Piece(PId, LId, dbTransaction, piece1, res);
                    retry++;

                }

                getCommitResult(dbTransaction, res);

                if (res.metrics != null) {
                    HashMap<String, String> newMetrics = new HashMap<>(res.metrics);
                    newMetrics.put("wasted", String.valueOf(wastedTime));
                    newMetrics.put("retry", String.valueOf(retry - 1));
                    res.setMetrics(newMetrics);
                }
                return res;
            } catch (Exception e) {
                rollback(dbTransaction);
                return res;
            }
        }
        return res;
    }

    public boolean runBuyListingIC3Piece(String PId, String LId, DBTransaction dbTransaction, IC3Piece piece1, TransactionResult res) throws Exception {
        dbService.ic3PieceBegin(dbTransaction, piece1);

//                Map<String, String> x = read(dbTransaction, "Items", "IId","1000");
//                write(dbTransaction, "Items", "IId", "1000", x);

        //            R from L where Lid
        Map<String, String> listing = read(dbTransaction, "Listings", "LId", LId);
//            Check player exists
        if (listing.isEmpty()) {
            log.info("listing does not exists!");
            return dbService.ic3PieceCommit(dbTransaction, piece1);
        }

        //            Read from P where pid

        Map<String, String> player = read(dbTransaction, "Players", "PId", PId);

//            Check player exists
        if (player.isEmpty()) {
            log.info("player does not exists!");
            return dbService.ic3PieceCommit(dbTransaction, piece1);
        }

        //            Check players cash for listing
        if (Double.parseDouble(player.get("PCash")) < Double.parseDouble(listing.get("LPrice"))) {
            log.info("player does not have enough cash");
            return dbService.ic3PieceCommit(dbTransaction, piece1);
        }


//            Read from I where LIId
        Map<String, String> item = read(dbTransaction, "Items", "IId", listing.get("LIId"));

        //            Check item exists
        if (item.isEmpty()) {
            log.info("item does not exists!");
            return dbService.ic3PieceCommit(dbTransaction, piece1);
        }


//            Delete from L where LID
        delete(dbTransaction, "Listings", "LId", LId);


//            R from P where ppid
        String PPId = item.get("IOwner");
        Map<String, String> prevOwner = read(dbTransaction, "Players", "PId", PPId);
        //            Check prevOwner exists
        if (prevOwner.isEmpty()) {
            log.info("previous owner does not exists!");
            return dbService.ic3PieceCommit(dbTransaction, piece1);
        }

//          W into I where IId = LIId SET IOwner = pid
        write(dbTransaction, "Items", "IId", listing.get("LIId"), "IOwner", PId);

//           W into P where Pid SET PCash = new Cash
        String newCash = Double.toString(Double.parseDouble(player.get("PCash")) - Double.parseDouble(listing.get("LPrice")));
        write(dbTransaction, "Players", "PId", PId, "PCash", newCash);

//           W into P where Pid SET PCash = new Cash
        String prevOwnerNewCash = Double.toString(Double.parseDouble(prevOwner.get("PCash")) + Double.parseDouble(listing.get("LPrice")));
        write(dbTransaction, "Players", "PId", PPId, "PCash", prevOwnerNewCash);


//                TODO: implement grpc call
        boolean pieceResult = dbService.ic3PieceCommit(dbTransaction, piece1);
        if (pieceResult)
            res.setMessage(listing.get("LIId"));
        return pieceResult;
    }
    public TransactionResult buyListingWW(String PId, String LId, String txId) {

        TransactionResult res = new TransactionResult();
        DBTransaction dbTransaction = null;
        try {
            dbTransaction = dbService.getTransaction(Data.newBuilder().setTransactionId(txId).build());
            res.setTxId(dbTransaction.getTimestamp());
        } catch (SQLException e) {
            return res;
        }

        if (dbTransaction != null) {
            try {

//                Map<String, String> x = read(dbTransaction, "Items", "IId","1000");
//                write(dbTransaction, "Items", "IId", "1000", x);

                //            R from L where Lid
                lock(dbTransaction, "Listings", "LId", LId, READ_TYPE);

                Map<String, String> listing = read(dbTransaction, "Listings", "LId", LId);
//            Check player exists
                if (listing.isEmpty()) {
                    log.info("listing does not exists!");
                    commit(dbTransaction);
                    return res;
                }

                //            Read from P where pid
                lock(dbTransaction, "Players", "PId", PId, READ_TYPE);

//            TODO: WE MIGHT NEED TO ROLLBACK
                Map<String, String> player = read(dbTransaction, "Players", "PId", PId);

//            Check player exists
                if (player.isEmpty()) {
                    log.info("player does not exists!");
                    commit(dbTransaction);
                    return res;
                }

                //            Check players cash for listing
                if (Double.parseDouble(player.get("PCash")) < Double.parseDouble(listing.get("LPrice"))) {
                    log.info("player does not have enough cash");
                    commit(dbTransaction);
                    return res;
                }


//            Read from I where LIId
                lock(dbTransaction, "Items", "IId", listing.get("LIId"), READ_TYPE);
                Map<String, String> item = read(dbTransaction, "Items", "IId", listing.get("LIId"));

                //            Check item exists
                if (item.isEmpty()) {
                    log.info("item does not exists!");
                    commit(dbTransaction);
                    return res;
                }


//            Delete from L where LID
                lock(dbTransaction, "Listings", "LId", LId, WRITE_TYPE);
                delete(dbTransaction, "Listings", "LId", LId);


//            R from P where ppid
                String PPId = item.get("IOwner");
                lock(dbTransaction, "Players", "PId", PPId, READ_TYPE);

                Map<String, String> prevOwner = read(dbTransaction, "Players", "PId", PPId);
                //            Check prevOwner exists
                if (prevOwner.isEmpty()) {
                    log.info("previous owner does not exists!");
                    commit(dbTransaction);
                    return res;
                }

//          W into I where IId = LIId SET IOwner = pid
                lock(dbTransaction, "Items", "IId", listing.get("LIId"), WRITE_TYPE);
                write(dbTransaction, "Items", "IId", listing.get("LIId"), "IOwner", PId);

//           W into P where Pid SET PCash = new Cash
                lock(dbTransaction, "Players", "PId", PId, WRITE_TYPE);
                String newCash = Double.toString(Double.parseDouble(player.get("PCash")) - Double.parseDouble(listing.get("LPrice")));
                write(dbTransaction, "Players", "PId", PId, "PCash", newCash);

//           W into P where Pid SET PCash = new Cash
                lock(dbTransaction, "Players", "PId", PPId, WRITE_TYPE).getStatus();
                String prevOwnerNewCash = Double.toString(Double.parseDouble(prevOwner.get("PCash")) + Double.parseDouble(listing.get("LPrice")));
                write(dbTransaction, "Players", "PId", PPId, "PCash", prevOwnerNewCash);

//            Unlock
                getCommitResult(dbTransaction, res);
                res.setMessage(listing.get("LIId"));


//                this.pidAccessed.computeIfPresent(PId + PPId, (key, value) -> value + 1);
//                this.pidAccessed.computeIfPresent(PPId, (key, value) -> value + 1);
//                this.pidAccessed.putIfAbsent(PId, 1L);
//                this.pidAccessed.putIfAbsent(PId + PPId, 1L);

                return res;
            } catch (Exception e) {
                rollback(dbTransaction);
                return res;
            }
        }
        return res;
    }


    public TransactionResult buyListingSLW(String PId, String LId, String txId) {

        TransactionResult res = new TransactionResult();
        log.info("buy listing <PID:{}, LID:{}>", PId, LId);
        DBTransaction dbTransaction = null;
        try {
            dbTransaction = dbService.getTransaction(Data.newBuilder().setTransactionId(txId).build());
            res.setTxId(dbTransaction.getTimestamp());
        } catch (SQLException e) {
            return res;
        }
        if (dbTransaction != null) {


            try {
                //            R from L where Lid
                lock(dbTransaction, "Listings", "LId", LId);

                Map<String, String> listing = read(dbTransaction, "Listings", "LId", LId);

//            Check player exists
                if (listing.isEmpty()) {
                    log.error("listing does not exists!");
                    commit(dbTransaction);
                    return res;
                }

//            Read from I where LIId
                lock(dbTransaction, "Items", "IId", listing.get("LIId"));

                Map<String, String> item = read(dbTransaction, "Items", "IId", listing.get("LIId"));

                //            Check item exists
                if (item.isEmpty()) {
                    log.error("item does not exists!");
                    commit(dbTransaction);
                    return res;
                }

//            Lock Players where pid, ppid
                String PPId = item.get("IOwner");
//                if (Integer.parseInt(PPId) > Integer.parseInt(PId)) {
//                    lock(dbTransaction, "Players", "PId", PId);
//                    lock(dbTransaction, "Players", "PId", PPId);
//                } else {
//                    lock(dbTransaction, "Players", "PId", PPId);
//                    lock(dbTransaction, "Players", "PId", PId);
//                }
                atomicLock(dbTransaction, "Players", "PId", List.of(PId, PPId));

                //            Read from P where pid
                Map<String, String> player = read(dbTransaction, "Players", "PId", PId);
//            Check player exists
                if (player.isEmpty()) {
                    log.error("player does not exists!");
                    commit(dbTransaction);
                    return res;
                }

                //            Check players cash for listing
                if (Double.parseDouble(player.get("PCash")) < Double.parseDouble(listing.get("LPrice"))) {
                    log.error("player does not have enough cash");
                    commit(dbTransaction);
                    return res;
                }


                Map<String, String> prevOwner = read(dbTransaction, "Players", "PId", PPId);
                //            Check prevOwner exists
                if (prevOwner.isEmpty()) {
                    log.error("previous owner does not exists!");
                    commit(dbTransaction);
                    return res;
                }


//            Delete from L where LID
                delete(dbTransaction, "Listings", "LId", LId);
               if (!Performance.INTERACTIVE && Performance.PARTIAL_CHOPPING)
                    unlock(dbTransaction, "Listings", "LId", LId);

//          W into I where IId = LIId SET IOwner = pid
                write(dbTransaction, "Items", "IId", listing.get("LIId"), "IOwner", PId);
               if (!Performance.INTERACTIVE && Performance.PARTIAL_CHOPPING)
                    unlock(dbTransaction, "Items", "IId", listing.get("LIId"));

//            delay(3000);
//           W into P where Pid SET PCash = new Cash
                String newCash = Double.toString(Double.parseDouble(player.get("PCash")) - Double.parseDouble(listing.get("LPrice")));
                write(dbTransaction, "Players", "PId", PId, "PCash", newCash);

//           W into P where Pid SET PCash = new Cash
                String prevOwnerNewCash = Double.toString(Double.parseDouble(prevOwner.get("PCash")) + Double.parseDouble(listing.get("LPrice")));
                write(dbTransaction, "Players", "PId", PPId, "PCash", prevOwnerNewCash);
//            Unlock
                getCommitResult(dbTransaction, res);
                res.setMessage(listing.get("LIId"));

//                this.pidAccessed.computeIfPresent(PId, (key, value) -> value + 1);
//                this.pidAccessed.computeIfPresent(PPId, (key, value) -> value + 1);
//                this.pidAccessed.putIfAbsent(PId, 1L);
//                this.pidAccessed.putIfAbsent(PPId, 1L);
//


                return res;
            } catch (Exception e) {
                log.error(e.getMessage());
                rollback(dbTransaction);
                return res;
            }
        }
        return res;
    }

    public TransactionResult buyListingORDERED(String PId, String LId, String txId) {

        TransactionResult res = new TransactionResult();
        log.info("buy listing <PID:{}, LID:{}>", PId, LId);
        DBTransaction dbTransaction = null;
        try {
            dbTransaction = dbService.getTransaction(Data.newBuilder().setTransactionId(txId).build());
            res.setTxId(dbTransaction.getTimestamp());
        } catch (SQLException e) {
            return res;
        }
        if (dbTransaction != null) {


            try {
                //            R from L where Lid
                lock(dbTransaction, "Listings", "LId", LId);



                Map<String, String> listing = read(dbTransaction, "Listings", "LId", LId);
                lock(dbTransaction, "Items", "IId", listing.get("LIId"));

//            Check player exists
                if (listing.isEmpty()) {
                    log.error("listing does not exists!");
                    commit(dbTransaction);
                    return res;
                }

//            Read from I where LIId


                Map<String, String> item = read(dbTransaction, "Items", "IId", listing.get("LIId"));


                //            Check item exists
                if (item.isEmpty()) {
                    log.error("item does not exists!");
                    commit(dbTransaction);
                    return res;
                }

//            Lock Players where pid, ppid
                String PPId = item.get("IOwner");
                if (Integer.parseInt(PPId) > Integer.parseInt(PId)) {
                    lock(dbTransaction, "Players", "PId", PId);
                    lock(dbTransaction, "Players", "PId", PPId);
                } else {
                    lock(dbTransaction, "Players", "PId", PPId);
                    lock(dbTransaction, "Players", "PId", PId);
                }

                //            Read from P where pid
                Map<String, String> player = read(dbTransaction, "Players", "PId", PId);
//            Check player exists
                if (player.isEmpty()) {
                    log.error("player does not exists!");
                    commit(dbTransaction);
                    return res;
                }

                //            Check players cash for listing
                if (Double.parseDouble(player.get("PCash")) < Double.parseDouble(listing.get("LPrice"))) {
                    log.error("player does not have enough cash");
                    commit(dbTransaction);
                    return res;
                }


                Map<String, String> prevOwner = read(dbTransaction, "Players", "PId", PPId);
                //            Check prevOwner exists
                if (prevOwner.isEmpty()) {
                    log.error("previous owner does not exists!");
                    commit(dbTransaction);
                    return res;
                }


//            Delete from L where LID
                delete(dbTransaction, "Listings", "LId", LId);
//                if (!Performance.INTERACTIVE && Performance.PARTIAL_CHOPPING)
//                    unlock(dbTransaction, "Listings", "LId", LId);

//          W into I where IId = LIId SET IOwner = pid
                write(dbTransaction, "Items", "IId", listing.get("LIId"), "IOwner", PId);
//                if (!Performance.INTERACTIVE && Performance.PARTIAL_CHOPPING)
//                    unlock(dbTransaction, "Items", "IId", listing.get("LIId"));

//            delay(3000);
//           W into P where Pid SET PCash = new Cash
                String newCash = Double.toString(Double.parseDouble(player.get("PCash")) - Double.parseDouble(listing.get("LPrice")));
                write(dbTransaction, "Players", "PId", PId, "PCash", newCash);

//           W into P where Pid SET PCash = new Cash
                String prevOwnerNewCash = Double.toString(Double.parseDouble(prevOwner.get("PCash")) + Double.parseDouble(listing.get("LPrice")));
                write(dbTransaction, "Players", "PId", PPId, "PCash", prevOwnerNewCash);
//            Unlock
                getCommitResult(dbTransaction, res);
                res.setMessage(listing.get("LIId"));

//                this.pidAccessed.computeIfPresent(PId, (key, value) -> value + 1);
//                this.pidAccessed.computeIfPresent(PPId, (key, value) -> value + 1);
//                this.pidAccessed.putIfAbsent(PId, 1L);
//                this.pidAccessed.putIfAbsent(PPId, 1L);
//


                return res;
            } catch (Exception e) {
                log.error(e.getMessage());
                rollback(dbTransaction);
                return res;
            }
        }
        return res;
    }

    private Result unlock(String transactionId, String tableName, String key, String value) throws Exception {
        Data lockData = Data.newBuilder()
                .setTransactionId(transactionId)
                .setType(READ_TYPE)
                .setKey(tableName + "," + key + "," + value)
                .build();
        Result unlockResult = dbService.unlock(lockData);
        log.info("{}, unlock {},{}:{} status: {} - message: {}",transactionId, tableName, key, value, unlockResult.getStatus(), unlockResult.getMessage());
        if (locks.containsKey(transactionId)) {
            locks.get(transactionId).remove(lockData.getKey());
        }
        if (!unlockResult.getStatus())
            throw new Exception(unlockResult.getMessage());
        return unlockResult;
    }

    private Result unlock(DBTransaction transaction, String tableName, String key, String value) throws Exception {
        Data lockData = Data.newBuilder()
                .setTransactionId(transaction.getTimestamp())
                .setType(READ_TYPE)
                .setKey(tableName + "," + key + "," + value)
                .build();
        Result unlockResult = dbService.unlock(lockData, transaction);
        log.info("{}, unlock {},{}:{} status: {} - message: {}",transaction.getUnlockingTime(), tableName, key, value, unlockResult.getStatus(), unlockResult.getMessage());
        return unlockResult;
    }

    private Result performRemoteOperation(Data data) {
        return dbService.update(data);
    }

    private Result performRemoteOperation(Data data, DBTransaction tx) {
        return dbService.update(tx, data);
    }


    private Result commit(DBTransaction tx) {
        Result commitResult = dbService.commitTransaction(tx);

        if (!commitResult.getStatus())
            log.error("commit {}: {}, {}", tx.getTimestamp(), commitResult.getStatus(), commitResult.getMessage());
        else
            log.info("commit {}: {}, {}", tx.getTimestamp(), commitResult.getStatus(), commitResult.getMessage());
        return commitResult;
    }




    private void write(DBTransaction transaction, String tableName, String key, String value, String newKey, String newValue) throws Exception {
        Data writeData = Data.newBuilder()
                .setTransactionId(transaction.getTimestamp())
                .setType(WRITE_TYPE)
                .setKey(tableName + "," + key + "," + value)
                .setValue(newKey + "," + newValue)
                .build();
        Result writeResult = performRemoteOperation(writeData, transaction);
        log.info("Write to {} status: {}", tableName, writeResult.getStatus());
        if (!writeResult.getStatus())
            throw new Exception(writeResult.getMessage());
    }

    private void write(DBTransaction transaction, String tableName, String key, String value, Map<String, String> newValue) throws Exception {
        Data writeData = Data.newBuilder()
                .setTransactionId(transaction.getTimestamp())
                .setType(WRITE_TYPE)
                .setKey(tableName + "," + key + "," + value)
                .putAllNewValue(newValue)
                .build();
        Result writeResult = dbService.update(transaction, writeData);
        log.info("Write to {} status: {}", tableName, writeResult.getStatus());
        if (!writeResult.getStatus())
            throw new Exception(writeResult.getMessage());
    }



    private void delete(DBTransaction dbTransaction, String tableName, String key, String value) throws Exception {
        Data deleteData = Data.newBuilder()
                .setTransactionId(dbTransaction.getTimestamp())
                .setType(DELETE_TYPE)
                .setKey(tableName + "," + key + "," + value)
                .build();
        Result deleteResult = performRemoteOperation(deleteData, dbTransaction);
        if (!deleteResult.getStatus())
            throw new Exception(deleteResult.getMessage());
        log.info("delete from {} status: {}", tableName, deleteResult.getStatus());
    }



    private Map<String, String> read(DBTransaction transaction, String tableName, String key, String value)  {
        Data readData = Data.newBuilder()
                .setTransactionId(transaction.getTimestamp())
                .setType(READ_TYPE)
                .setKey(tableName + "," + key + "," + value)
                .build();
        Result readResult = performRemoteOperation(readData, transaction);
        log.info("{}: read from {} status : {}",transaction.getTimestamp(), tableName, readResult.getStatus());
        if (!readResult.getStatus())
            log.error("{}: read from {} error: {}", transaction.getTimestamp(), tableName, readResult.getMessage());
        return readResult.getStatus() ? readResult.getReturnsMap() : new HashMap<>();
    }




    private void insert(DBTransaction dbTransaction, String tableName, String newRecord, String recordId) throws Exception {
        Data insertData = Data.newBuilder()
                .setTransactionId(dbTransaction.getTimestamp())
                .setType(INSERT_TYPE)
                .setKey(tableName)
                .setValue(newRecord)
                .setRecordId(recordId)
                .build();
        Result result = dbService.update(dbTransaction, insertData);
        log.info("insert data with id {} into {} status : {}", recordId, tableName, result.getStatus());
        if (!result.getStatus())
            throw new Exception(result.getMessage());
    }

    private void insert(DBTransaction dbTransaction, String tableName, Map<String, String> newRecord, String recordId) throws Exception {
        Data insertData = Data.newBuilder()
                .setTransactionId(dbTransaction.getTimestamp())
                .setType(INSERT_TYPE)
                .setKey(tableName)
                .putAllNewValue(newRecord)
                .setRecordId(recordId)
                .build();
        Result result = dbService.update(dbTransaction, insertData);
        log.info("insert data with id {} into {} status : {}", recordId, tableName, result.getStatus());
        if (!result.getStatus())
            throw new Exception(result.getMessage());
    }

    private void insert(DBTransaction dbTransaction, String tableName, String newRecord, String recordId, String recordKeys) throws Exception {
        Data insertData = Data.newBuilder()
                .setTransactionId(dbTransaction.getTimestamp())
                .setType(INSERT_TYPE)
                .setKey(tableName)
                .setValue(newRecord)
                .setRecordId(recordId)
                .setRecordKeys(recordKeys)
                .build();
        Result result = dbService.update(dbTransaction, insertData);
        log.info("insert data with id {} into {} status : {}", recordId, tableName, result.getStatus());
        if (!result.getStatus())
            throw new Exception(result.getMessage());
    }


//    private Result lock(DBTransaction transaction, String tableName, String key, String value, String type) throws Exception {
//        Data lockData = Data.newBuilder()
//                .setTransactionId(transaction.getTimestamp())
//                .setType(type)
//                .setKey(tableName + "," + key + "," + value)
//                .build();
//        Result lockResult = dbService.lock(transaction, lockData);
//        log.info("{}, lock on {},{}:{} status: {} - message: {}", transaction.getTimestamp(), tableName, key, value, lockResult.getStatus(), lockResult.getMessage());
//        if (!lockResult.getStatus())
//            throw new Exception(lockResult.getMessage());
//        return lockResult;
//    }

    private Result lock(DBTransaction transaction, String tableName, String key, String value, String type) throws Exception {
        LockData lockData = LockData.newBuilder()
                .setTransactionId(transaction.getTimestamp())
                .setType(type)
                .setResource(tableName + "," + value)
                .setLockType(WRITE_TYPE.equals(type) ? LockType.WRITE : LockType.READ)
                .build();
        Result lockResult = dbService.lock(transaction, lockData);
        log.info("{}, lock on {}:{} status: {} - message: {}", transaction.getTimestamp(), tableName, value, lockResult.getStatus(), lockResult.getMessage());
        if (!lockResult.getStatus())
            throw new Exception(lockResult.getMessage());
        return lockResult;
    }

    private Result atomicLock(DBTransaction transaction, String tableName, String key, List<String> values) throws Exception {
        Result lockResult = dbService.atomicLock(transaction, tableName, key, values);
        log.info("{}, lock on {},{}:{} status: {} - message: {}", transaction.getTimestamp(), tableName, key, values, lockResult.getStatus(), lockResult.getMessage());
        if (!lockResult.getStatus())
            throw new Exception(lockResult.getMessage());
        return lockResult;
    }

    private Result tryLock(DBTransaction transaction, String tableName, String key, String value, String type) throws Exception {
        Data lockData = Data.newBuilder()
                .setTransactionId(transaction.getTimestamp())
                .setType(type)
                .setKey(tableName + "," + key + "," + value)
                .build();
        Result lockResult = dbService.tryLock(transaction, lockData);
        log.info("{}, lock on {},{}:{} status: {} - message: {}", transaction.getTimestamp(), tableName, key, value, lockResult.getStatus(), lockResult.getMessage());
        return lockResult;
    }



    private Result lock(DBTransaction tx, String tableName, String key, String value) throws Exception {
        return lock(tx, tableName, key, value, WRITE_TYPE);
    }


    private Result insertLock(DBTransaction tx, String tableName) {
        Data lockData = Data.newBuilder()
                .setTransactionId(tx.getTimestamp())
                .setType(INSERT_TYPE)
                .setKey(tableName)
                .build();
        Result lockResult = dbService.lock(tx, lockData);
        log.info("lock on {} for insert status: {}", tableName, lockResult.getStatus());
        return lockResult;
    }

    private Result insertLock(DBTransaction tx, String tableName, String recordId) throws Exception {
        return lock(tx, tableName, "", recordId);
    }

    public static Map<String, String> convertStringToMap(String str) {
        Map<String, String> map = new HashMap<>();

        // Split the string into key-value pairs
        String[] pairs = str.split(",");

        for (String pair : pairs) {
            // Split each pair to get key and value
            String[] keyValue = pair.split(":", 2);
            if (keyValue.length == 2) { // Check if there's a key and a value
                map.put(keyValue[0], keyValue[1]);
            }
        }

        return map;
    }
}
