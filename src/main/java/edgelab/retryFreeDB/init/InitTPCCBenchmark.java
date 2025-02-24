package edgelab.retryFreeDB.init;

import com.fasterxml.jackson.databind.ObjectMapper;
import edgelab.retryFreeDB.benchmark.util.TPCCUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.rocksdb.Checkpoint;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class InitTPCCBenchmark {

    private static final int FIRST_UNPROCESSED_O_ID = 2101;
    private static final int BATCH_SIZE = 10000;
    private static final int CONFIG_ITEM_COUNT = 100000;
    private static final int CONFIG_CUST_PER_DISTRICT = 3000;
    private static final int CONFIG_DIST_PER_WAREHOUSE = 10;

    private static long numWarehouses = 1;

    public static RocksDB db;
    public static Random rng = new Random(123);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) {
        if (args.length <= 1) {
            System.out.println("<checkpoint_dir> <num_warehouse>");
            System.exit(1);
        }

        String checkpointDir = args[0];
        numWarehouses = Integer.parseInt(args[1]);

        String dbPath;
        try {
            dbPath = Files.createTempDirectory("rocksdb-tmp").toString();
        } catch (IOException e) {
            throw new RuntimeException("Failed to create temporary directory for RocksDB", e);
        }

        System.out.println("Database path: " + dbPath);
        System.out.println("Checkpoint directory: " + checkpointDir);

        RocksDB.loadLibrary();
        try (Options options = new Options().setCreateIfMissing(true).setWriteBufferSize(16L * 1024 * 1024 * 1024)) {
            db = RocksDB.open(options, dbPath);


            load();

            // Create checkpoint
            createCheckpoint(db, checkpointDir);
            db.close();
        } catch (RocksDBException e) {
            e.printStackTrace();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void createCheckpoint(RocksDB db, String checkpointDir) throws RocksDBException, IOException {
        System.out.println("Creating checkpoint...");
        File checkpointDirectory = new File(checkpointDir);
        if (checkpointDirectory.exists()) {
            FileUtils.deleteDirectory(checkpointDirectory);
        }
        Checkpoint checkpoint = Checkpoint.create(db);
        checkpoint.createCheckpoint(checkpointDir);
        System.out.println("Checkpoint created at: " + checkpointDir);
    }

    public InitTPCCBenchmark() {
    }

    public static void load() throws RocksDBException {
        loadItems(CONFIG_ITEM_COUNT);

        // WAREHOUSES
        // We use a separate thread per warehouse. Each thread will load
        // all of the tables that depend on that warehouse. They all have
        // to wait until the ITEM table is loaded first though.
        ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        CountDownLatch latch = new CountDownLatch((int) numWarehouses);

        for (int w = 1; w <= numWarehouses; w++) {
            final int w_id = w;
            executor.submit(() -> {
                try {
                    log.info("Starting to load WAREHOUSE {}", w_id);
                    loadWarehouse(w_id);
                    log.info("Starting to load STOCK {}", w_id);
                    loadStock(w_id, CONFIG_ITEM_COUNT);
                    log.info("Starting to load DISTRICT {}", w_id);
                    loadDistricts(w_id, CONFIG_DIST_PER_WAREHOUSE);
                    log.info("Starting to load CUSTOMER {}", w_id);
                    loadCustomers(w_id, CONFIG_DIST_PER_WAREHOUSE, CONFIG_CUST_PER_DISTRICT);
                    log.info("Starting to load CUSTOMER HISTORY {}", w_id);
                    loadCustomerHistory(w_id, CONFIG_DIST_PER_WAREHOUSE, CONFIG_CUST_PER_DISTRICT);
                    log.info("Starting to load ORDERS {}", w_id);
                    loadOpenOrders(w_id, CONFIG_DIST_PER_WAREHOUSE, CONFIG_CUST_PER_DISTRICT);
                    log.info("Starting to load NEW ORDERS {}", w_id);
                    loadNewOrders(w_id, CONFIG_DIST_PER_WAREHOUSE, CONFIG_CUST_PER_DISTRICT);
                    log.info("Starting to load ORDER LINES {}", w_id);
                    loadOrderLines(w_id, CONFIG_DIST_PER_WAREHOUSE, CONFIG_CUST_PER_DISTRICT);
                } catch (RocksDBException e) {
                    log.error("Error loading warehouse {}: {}", w_id, e.getMessage(), e);
                } finally {
                    latch.countDown(); // Decrease latch count when warehouse processing is done
                }
            });
        }

        try {
            latch.await(); // Wait for all threads to complete
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Warehouse loading interrupted", e);
        }

        executor.shutdown();
    }

    private static byte[] objectToJson(Object obj) {
        try {
            return objectMapper.writeValueAsBytes(obj);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    protected static void loadItems(int itemCount) throws RocksDBException {
        int batchSize = 0;
        WriteBatch writeBatch = new WriteBatch();
        for (int i = 1; i <= itemCount; i++) {

            Map<String, String> item = new HashMap<>();
            String i_id = String.valueOf(i);
            item.put("i_name" , TPCCUtil.randomStr(TPCCUtil.randomNumber(14, 24, rng)));
            item.put("i_price" , String.valueOf(TPCCUtil.randomNumber(100, 10000, rng) / 100.0));

            // i_data
            int randPct = TPCCUtil.randomNumber(1, 100, rng);
            int len = TPCCUtil.randomNumber(26, 50, rng);
            if (randPct > 10) {
                // 90% of time i_data isa random string of length [26 .. 50]
                item.put("i_data", TPCCUtil.randomStr(len));
            } else {
                // 10% of time i_data has "ORIGINAL" crammed somewhere in
                // middle
                int startORIGINAL = TPCCUtil.randomNumber(2, (len - 8), rng);
                item.put("i_data",
                        TPCCUtil.randomStr(startORIGINAL - 1)
                                + "ORIGINAL"
                                + TPCCUtil.randomStr(len - startORIGINAL - 9));
            }

            item.put("i_im_id",  String.valueOf(TPCCUtil.randomNumber(1, 10000, rng)));


            String key = "item:" + i_id;
            writeBatch.put(key.getBytes(), objectToJson(item));
            batchSize++;

            if (batchSize == BATCH_SIZE) {
                db.write(new WriteOptions(), writeBatch);
                writeBatch = new WriteBatch();
                batchSize = 0;
            }
        }

        if (batchSize > 0) {
            db.write(new WriteOptions(), writeBatch);
        }

    }


    private static void loadWarehouse(int w_id) throws RocksDBException {
        Map<String, String> warehouse = new HashMap<>();
        warehouse.put("w_id", String.valueOf(w_id));
        warehouse.put("w_ytd", "300000");
        warehouse.put("w_tax", String.valueOf(TPCCUtil.randomNumber(0, 2000, rng) / 10000.0));
        warehouse.put("w_name", TPCCUtil.randomStr(TPCCUtil.randomNumber(6, 10, rng)));
        warehouse.put("w_street_1", TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, rng)));
        warehouse.put("w_street_2", TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, rng)));
        warehouse.put("w_city", TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, rng)));
        warehouse.put("w_state", TPCCUtil.randomStr(3).toUpperCase());
        warehouse.put("w_zip", "123456789");

        db.put(("warehouse:" + w_id).getBytes(), objectToJson(warehouse));
    }
    protected static void loadStock(int w_id, int numItems) throws RocksDBException {
        WriteBatch writeBatch = new WriteBatch();
        int batchSize = 0;
        for (int i = 1; i <= numItems; i++) {
            Map<String, String> stock = new HashMap<>();
            stock.put("s_w_id", String.valueOf(w_id));
            stock.put("s_i_id", String.valueOf(i));
            stock.put("s_quantity", String.valueOf(TPCCUtil.randomNumber(10, 100, rng)));
            stock.put("s_ytd", "0");
            stock.put("s_order_cnt", "0");
            stock.put("s_remote_cnt", "0");

            int randPct = TPCCUtil.randomNumber(1, 100, rng);
            int len = TPCCUtil.randomNumber(26, 50, rng);
            if (randPct > 10) {
                stock.put("s_data", TPCCUtil.randomStr(len));
            } else {
                int startORIGINAL = TPCCUtil.randomNumber(2, (len - 8), rng);
                stock.put("s_data", TPCCUtil.randomStr(startORIGINAL - 1) + "ORIGINAL" + TPCCUtil.randomStr(len - startORIGINAL - 9));
            }

            for (int j = 1; j <= 10; j++) {
                stock.put("s_dist_" + j, TPCCUtil.randomStr(24));
            }

            writeBatch.put(("stock:" + w_id + "," + i).getBytes(), objectToJson(stock));
            batchSize++;

            if (batchSize == BATCH_SIZE) {
                db.write(new WriteOptions(), writeBatch);
                writeBatch = new WriteBatch();
                batchSize = 0;
            }
        }
        if (batchSize > 0) {
            db.write(new WriteOptions(), writeBatch);
        }
    }
    protected static void loadDistricts(int w_id, int districtsPerWarehouse) throws RocksDBException {
        for (int d = 1; d <= districtsPerWarehouse; d++) {
            Map<String, String> district = new HashMap<>();
            district.put("d_id", String.valueOf(d));
            district.put("d_w_id", String.valueOf(w_id));
            district.put("d_ytd", "30000");
            district.put("d_tax", String.valueOf(TPCCUtil.randomNumber(0, 2000, rng) / 10000.0));
            district.put("d_next_o_id", String.valueOf(CONFIG_CUST_PER_DISTRICT + 1));
            district.put("d_name", TPCCUtil.randomStr(TPCCUtil.randomNumber(6, 10, rng)));
            district.put("d_street_1", TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, rng)));
            district.put("d_street_2", TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, rng)));
            district.put("d_city", TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, rng)));
            district.put("d_state", TPCCUtil.randomStr(3).toUpperCase());
            district.put("d_zip", "123456789");

            db.put(("district:" + w_id + "," + d).getBytes(), objectToJson(district));
        }

    }

    protected static void loadCustomers(int w_id, int districtsPerWarehouse, int customersPerDistrict) throws RocksDBException {
        WriteBatch writeBatch = new WriteBatch();
        int batchSize = 0;
        for (int d = 1; d <= districtsPerWarehouse; d++) {
            for (int c = 1; c <= customersPerDistrict; c++) {
                Timestamp sysdate = new Timestamp(System.currentTimeMillis());
                Map<String, String> customer = new HashMap<>();
                customer.put("c_id", String.valueOf(c));
                customer.put("c_d_id", String.valueOf(d));
                customer.put("c_w_id", String.valueOf(w_id));
                customer.put("c_discount", String.valueOf(TPCCUtil.randomNumber(1, 5000, rng) / 10000.0));
                customer.put("c_credit", TPCCUtil.randomNumber(1, 100, rng) <= 10 ? "BC" : "GC");
                customer.put("c_last", c <= 1000 ? TPCCUtil.getLastName(c - 1) : TPCCUtil.getNonUniformRandomLastNameForLoad(rng));
                customer.put("c_first", TPCCUtil.randomStr(TPCCUtil.randomNumber(8, 16, rng)));
                customer.put("c_credit_lim", "50000");
                customer.put("c_balance", "-10");
                customer.put("c_ytd_payment", "10");
                customer.put("c_payment_cnt", "1");
                customer.put("c_delivery_cnt", "0");
                customer.put("c_street_1", TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, rng)));
                customer.put("c_street_2", TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, rng)));
                customer.put("c_city", TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 20, rng)));
                customer.put("c_state", TPCCUtil.randomStr(3).toUpperCase());
                customer.put("c_zip", TPCCUtil.randomNStr(4) + "11111");
                customer.put("c_phone", TPCCUtil.randomNStr(16));
                customer.put("c_since", sysdate.toString());
                customer.put("c_middle", "OE");
                customer.put("c_data", TPCCUtil.randomStr(TPCCUtil.randomNumber(300, 500, rng)));

                writeBatch.put(("customer:" + w_id + "," + d + "," + c).getBytes(), objectToJson(customer));
                batchSize++;

                if (batchSize == BATCH_SIZE) {
                    db.write(new WriteOptions(), writeBatch);
                    writeBatch = new WriteBatch();
                    batchSize = 0;
                }
            }
        }
        if (batchSize > 0) {
            db.write(new WriteOptions(), writeBatch);
        }
    }


    protected static void loadCustomerHistory(int w_id, int districtsPerWarehouse, int customersPerDistrict) throws RocksDBException {
        WriteBatch writeBatch = new WriteBatch();
        int batchSize = 0;
        for (int d = 1; d <= districtsPerWarehouse; d++) {
            for (int c = 1; c <= customersPerDistrict; c++) {
                Timestamp sysdate = new Timestamp(System.currentTimeMillis());
                Map<String, String> history = new HashMap<>();
                history.put("h_c_id", String.valueOf(c));
                history.put("h_c_d_id", String.valueOf(d));
                history.put("h_c_w_id", String.valueOf(w_id));
                history.put("h_d_id", String.valueOf(d));
                history.put("h_w_id", String.valueOf(w_id));
                history.put("h_date", sysdate.toString());
                history.put("h_amount", "10");
                history.put("h_data", TPCCUtil.randomStr(TPCCUtil.randomNumber(10, 24, rng)));

                writeBatch.put(("history:" + w_id + "," + d + "," + c).getBytes(), objectToJson(history));
                batchSize++;

                if (batchSize == BATCH_SIZE) {
                    db.write(new WriteOptions(), writeBatch);
                    writeBatch = new WriteBatch();
                    batchSize = 0;
                }
            }
        }
        if (batchSize > 0) {
            db.write(new WriteOptions(), writeBatch);
        }
    }

    protected static void loadOpenOrders(int w_id, int districtsPerWarehouse, int customersPerDistrict) throws RocksDBException {
        WriteBatch writeBatch = new WriteBatch();
        int batchSize = 0;
        for (int d = 1; d <= districtsPerWarehouse; d++) {
            int[] c_ids = new int[customersPerDistrict];
            for (int i = 0; i < customersPerDistrict; ++i) {
                c_ids[i] = i + 1;
            }
            for (int i = 0; i < c_ids.length - 1; ++i) {
                int remaining = c_ids.length - i - 1;
                int swapIndex = rng.nextInt(remaining) + i + 1;
                int temp = c_ids[swapIndex];
                c_ids[swapIndex] = c_ids[i];
                c_ids[i] = temp;
            }
            for (int c = 1; c <= customersPerDistrict; c++) {
                Timestamp sysdate = new Timestamp(System.currentTimeMillis());
                Map<String, String> order = new HashMap<>();
                order.put("o_id", String.valueOf(c));
                order.put("o_w_id", String.valueOf(w_id));
                order.put("o_d_id", String.valueOf(d));
                order.put("o_c_id", String.valueOf(c_ids[c - 1]));
                order.put("o_carrier_id", c < FIRST_UNPROCESSED_O_ID ? String.valueOf(TPCCUtil.randomNumber(1, 10, rng)) : "NULL");
                order.put("o_ol_cnt", String.valueOf(getRandomCount(w_id, c, d)));
                order.put("o_all_local", "1");
                order.put("o_entry_d", sysdate.toString());

                writeBatch.put(("oorder:" + w_id + "," + d + "," + c).getBytes(), objectToJson(order));
                batchSize++;

                if (batchSize == BATCH_SIZE) {
                    db.write(new WriteOptions(), writeBatch);
                    writeBatch = new WriteBatch();
                    batchSize = 0;
                }
            }
        }
        if (batchSize > 0) {
            db.write(new WriteOptions(), writeBatch);
        }
    }
    private static int getRandomCount(int w_id, int c, int d) {
        String customer = "" + w_id + c + d;
        Random random = new Random(customer.hashCode());

        return TPCCUtil.randomNumber(5, 15, random);
    }



    protected static void loadNewOrders(int w_id, int districtsPerWarehouse, int customersPerDistrict) throws RocksDBException {
        WriteBatch writeBatch = new WriteBatch();
        int batchSize = 0;
        for (int d = 1; d <= districtsPerWarehouse; d++) {
            for (int c = FIRST_UNPROCESSED_O_ID; c <= customersPerDistrict; c++) {
                Map<String, String> newOrder = new HashMap<>();
                newOrder.put("no_w_id", String.valueOf(w_id));
                newOrder.put("no_d_id", String.valueOf(d));
                newOrder.put("no_o_id", String.valueOf(c));

                writeBatch.put(("new_order:" + w_id + "," + d + "," + c).getBytes(), objectToJson(newOrder));
                batchSize++;

                if (batchSize == BATCH_SIZE) {
                    db.write(new WriteOptions(), writeBatch);
                    writeBatch = new WriteBatch();
                    batchSize = 0;
                }
            }
        }
        if (batchSize > 0) {
            db.write(new WriteOptions(), writeBatch);
        }
    }
    protected static void loadOrderLines(int w_id, int districtsPerWarehouse, int customersPerDistrict) throws RocksDBException {
        WriteBatch writeBatch = new WriteBatch();
        int batchSize = 0;
        for (int d = 1; d <= districtsPerWarehouse; d++) {
            for (int c = 1; c <= customersPerDistrict; c++) {
                int count = getRandomCount(w_id, c, d);
                for (int l = 1; l <= count; l++) {
                    Map<String, String> orderLine = new HashMap<>();
                    orderLine.put("ol_w_id", String.valueOf(w_id));
                    orderLine.put("ol_d_id", String.valueOf(d));
                    orderLine.put("ol_o_id", String.valueOf(c));
                    orderLine.put("ol_number", String.valueOf(l));
                    orderLine.put("ol_i_id", String.valueOf(TPCCUtil.randomNumber(1, CONFIG_ITEM_COUNT, rng)));
                    if (c < FIRST_UNPROCESSED_O_ID) {
                        orderLine.put("ol_delivery_d", new Timestamp(System.currentTimeMillis()).toString());
                        orderLine.put("ol_amount", "0");
                    } else {
                        orderLine.put("ol_delivery_d", "NULL");
                        orderLine.put("ol_amount", String.valueOf((float) TPCCUtil.randomNumber(1, 999999, rng) / 100.0));
                    }
                    orderLine.put("ol_supply_w_id", String.valueOf(w_id));
                    orderLine.put("ol_quantity", "5");
                    orderLine.put("ol_dist_info", TPCCUtil.randomStr(24));

                    writeBatch.put(("order_line:" + w_id + "," + d + "," + c + "," + l).getBytes(), objectToJson(orderLine));
                    batchSize++;

                    if (batchSize == BATCH_SIZE) {
                        db.write(new WriteOptions(), writeBatch);
                        writeBatch = new WriteBatch();
                        batchSize = 0;
                    }
                }
            }
        }
        if (batchSize > 0) {
            db.write(new WriteOptions(), writeBatch);
        }
    }
}