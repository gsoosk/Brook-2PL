package edgelab.retryFreeDB.benchmark;



import edgelab.proto.RetryFreeDBServerGrpc;
import edgelab.retryFreeDB.clients.Client;
import edgelab.retryFreeDB.clients.InteractiveClient;
import edgelab.retryFreeDB.benchmark.util.RandomGenerator;
import edgelab.retryFreeDB.benchmark.util.TPCCConfig;
import edgelab.retryFreeDB.benchmark.util.TPCCUtil;
import edgelab.retryFreeDB.clients.StoredProcedureClient;
import edgelab.retryFreeDB.init.InitStoreBenchmark;
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Summary;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.MutuallyExclusiveGroup;
import net.sourceforge.argparse4j.inf.Namespace;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.rmi.RemoteException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static net.sourceforge.argparse4j.impl.Arguments.store;
import static net.sourceforge.argparse4j.impl.Arguments.append;


@Slf4j
public class Performance {

    private abstract class ServerRequest {
        @Getter
        protected long transactionId;
        @Getter
        protected int retried;
        @Getter
        protected Map<String, String> values;
        @Getter
        protected Long start;
        public void retry() {
            this.retried++;
        }

        public ServerRequest(long batchId, Map<String, String> values, Long start) {
            this.transactionId = batchId;
            this.retried = 0;
            this.values = values;
            this.start = start;
        }

    }

    private class StoreServerRequest extends ServerRequest{
        enum Type {
            BUY,
            SELL,
            BUY_HOT,
            SELL_HOT
        }
        @Getter
        private Type type;

        public StoreServerRequest(Type type, long batchId, Map<String, String> values, Long start) {
            super(batchId, values, start);
            this.type = type;
        }
    }

    private class TPCCServerRequest extends ServerRequest {
        enum Type {
            PAYMENT,
            NEW_ORDER
        }

        @Getter
        private Type type;
        @Getter
        private boolean userAbort;

        @Getter
        @Setter
        Map<String, int[]> arrayValues = new HashMap<>();

        public TPCCServerRequest(Type type, long batchId, Map<String, String> values, Long start, boolean userAbort) {
            super(batchId, values, start);
            this.type = type;
            this.userAbort = userAbort;
        }
    }

    private static final Integer INFINITY = -1;

    private RetryFreeDBServerGrpc.RetryFreeDBServerBlockingStub datastore;
    private RetryFreeDBServerGrpc.RetryFreeDBServerStub asyncDatastore;
    private ManagedChannel channel;
    private void connectToDataStore(String address, int port) throws MalformedURLException, RemoteException {
        this.channel = ManagedChannelBuilder.forAddress(address, port).maxInboundMessageSize(Integer.MAX_VALUE).usePlaintext().build();
        ManagedChannel asyncChannel = ManagedChannelBuilder.forAddress(address, port).maxInboundMessageSize(Integer.MAX_VALUE).usePlaintext().build();
        this.datastore = RetryFreeDBServerGrpc.newBlockingStub(channel);
        this.asyncDatastore = RetryFreeDBServerGrpc.newStub(asyncChannel);

        while (true) {
            try {
                if (this.channel.getState(true) == ConnectivityState.READY &&
                        asyncChannel.getState(true) == ConnectivityState.READY) {
                    log.info("Connected to grpc server");
                    break;
                }
            } catch (Exception e) {
                System.out.println("Remote connection failed, trying again in 5 seconds");
                log.error("Remote connection failed", e);
                // wait for 5 seconds before trying to re-establish connection
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e1) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                }
            }
        }
    }

    private Namespace res;

    public static void main(String[] args) throws Exception{
        Performance perf = new Performance();

        perf.parseArguments(args);
        perf.start();

    }

    private void parseArguments(String[] args) throws ArgumentParserException {
        ArgumentParser parser = argParser();
        try {
            res = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            if (args.length == 0) {
                parser.printHelp();
            } else {
                parser.handleError(e);
            }
           throw e;
        }
    }


    private String address;
    private int port;

//    Multi-threading
    private int MAX_THREADS = 60;
    private int MAX_ITEM_READ_THREADS = 20;
    private final int MAX_QUEUE_SIZE = 5;
    private int MAX_RETRY = -1;
    private ThreadPoolExecutor executor;
    private ThreadPoolExecutor itemExecutor;
    private Client client;
    private long txId = 0;
    private Map<String, Set<String>> hotPlayersAndItems; // Player:{items}
    private List<String> hotItems = new ArrayList<>();
    private Map<String, List<String>> hotListings; // Listing: <iid, price>
    private Set<String> alreadyBoughtListing = new HashSet<>();
    private int nextNotHotListingID = 10;
    private int nextNotHotItem = (InitStoreBenchmark.NUM_OF_PLAYERS / 5) * InitStoreBenchmark.NUM_OF_EACH_PLAYER_ITEMS;
    private int HOT_RECORD_SELECTION_CHANCE = 0;
    private final int NUM_OF_PLAYERS = InitStoreBenchmark.NUM_OF_PLAYERS;
    private int NUM_OF_WAREHOUSES = 1;
    private int NUM_OF_HOT_WAREHOUSE = 0;

    private List<Integer> hotWarehouses = new ArrayList<>();
    private List<Integer> notHotWarehouses = new ArrayList<>();
    private int buy_or_sell = 1;
    private int buy_or_sell_hot = 1;
    private final RandomGenerator random = new RandomGenerator(1234);
    private static Map<String, Set<String>> readHotPlayerRecords(String filePath) {
        Map<String, Set<String>> map = new ConcurrentHashMap<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(","); // Change "," to your actual delimiter
                if (parts.length >= 2) {
                    String key = parts[1].trim(); // Second column as key
                    String value = parts[0].trim(); // First column as value

                    // Check if the key exists and add the value to its list
                    map.computeIfAbsent(key, k -> ConcurrentHashMap.newKeySet()).add(value);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return map;
    }


    private static Map<String, List<String>> readHotListingRecords(String filePath) {
        Map<String, List<String>> records = new ConcurrentHashMap<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(",");
                if (parts.length == 3) {
                    String firstColumn = parts[0].trim();
                    String secondColumn = parts[1].trim();
                    String thirdColumn = parts[2].trim();
                    records.put(firstColumn, List.of( secondColumn, thirdColumn));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return records;
    }

    private void removeAlreadyListedRecords() {
        for (String player : hotPlayersAndItems.keySet()) {
            hotPlayersAndItems.get(player).removeIf(s -> {
                for (String listing : hotListings.keySet()) {
                    if (hotListings.get(listing).get(0).equals(s))
                        return true;
                }
                return false;
            });
        }
    }

    void start() throws Exception {
        /* parse args */
        address = res.getString("address");
        port = res.getInt("port");

        Integer batchSize = res.getInt("batchSize");
        Double interval = res.getDouble("interval");

        List<Integer> dynamicBatchSize = res.getList("dynamicBatchSize");
        List<Integer> dynamicBatchTimes = res.getList("dynamicBatchTime");
        int currentBatchIndex = 0;
        boolean dynamicBatching = false;
        if (dynamicBatchSize != null && dynamicBatchTimes != null ) {
            if (dynamicBatchSize.size() != dynamicBatchTimes.size())
                throw new Exception("number of dynamic batch times and batch sizes should be equal");
            batchSize = dynamicBatchSize.get(currentBatchIndex);
            dynamicBatching = true;
        }
        List<Double> dynamicInterval = res.getList("dynamicInterval");
        List<Integer> dynamicIntervalTime = res.getList("dynamicIntervalTime");
        int currentIntervalIndex = 0;
        boolean dynamicIntervaling = false;
        if (dynamicInterval != null && dynamicIntervalTime != null ) {
            if (dynamicInterval.size() != dynamicIntervalTime.size())
                throw new Exception("number of dynamic interval times and intervals should be equal");
            interval = dynamicInterval.get(currentIntervalIndex);
            dynamicIntervaling = true;
        }

        Integer recordSize = res.getInt("recordSize") == null ? 255 : res.getInt("recordSize") ;
        long warmup = batchSize != null ? (batchSize / recordSize) * 4L : 100;
        long numRecords = res.getLong("numRecords") == null ? Integer.MAX_VALUE - warmup - 2 : res.getLong("numRecords");
        numRecords += warmup;
        int throughput = res.getInt("throughput");
        String payloadFilePath = res.getString("payloadFile");
        String resultFilePath = res.getString("resultFile") == null ? "./result/result.csv" : res.getString("resultFile") ;
        String metricsFilePath = res.getString("metricsFile") == null ? "metrics.csv" : res.getString("metricsFile") ;
        String partitionId = res.getString("partitionId");
        // since default value gets printed with the help text, we are escaping \n there and replacing it with correct value here.
        String payloadDelimiter = res.getString("payloadDelimiter").equals("\\n") ? "\n" : res.getString("payloadDelimiter");
        warmup = 10;
        final Long benchmarkTime = res.getLong("benchmarkTime") + warmup;
//        Integer timeout = res.getInt("timeout") != null ? res.getInt("timeout") : interval.intValue() * 2;

        Boolean exponentialLoad = res.getBoolean("exponentialLoad") == null ? false : res.getBoolean("exponentialLoad");

        List<Long> dynamicMemory = res.getList("dynamicMemory");
        List<Integer> dynamicMemoryTime = res.getList("dynamicMemoryTime");

        //         METRICS
        DefaultExports.initialize(); // export jvm
        HTTPServer metricServer = new HTTPServer.Builder()
                .withPort(9002)
                .build();


        String benchmarkMode = res.getString("benchmarkMode");

        this.HOT_RECORD_SELECTION_CHANCE = res.getInt("hotSelectionProb");
        this.NUM_OF_WAREHOUSES = res.getInt("numOfWarehouses");
        this.NUM_OF_HOT_WAREHOUSE = res.getInt("hotWarehouses");


        if (benchmarkMode.equals("store")) {
            hotPlayersAndItems = readHotPlayerRecords(res.getString("hotPlayers"));
            populateHotItems();
            hotListings = readHotListingRecords(res.getString("hotListings"));
            removeAlreadyListedRecords();
        } else if (benchmarkMode.equals("tpcc")) {
            populateHotWarehouses();
        }



        if (res.getInt("maxThreads") != null)
            this.MAX_THREADS = res.getInt("maxThreads");



        this.MAX_RETRY = res.getInt("maxRetry");
//        connectToDataStore(address, port);

        Map<String, String> serverConfig = new HashMap<>();
        int operationDelay = 10;
        if (res.getInt("operationDelay") != null) {
            serverConfig.put("operationDelay", res.getInt("operationDelay").toString());
            operationDelay = res.getInt("operationDelay");
        }
        serverConfig.put("mode", res.getString("2PLMode"));



        long startMs = System.currentTimeMillis();

        log.info("Running benchmark for partition: " + partitionId);



        executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(MAX_THREADS);


        Thread.sleep(3000);

        String transactionMode = res.getString("transactionMode");
        if (Objects.equals(transactionMode, "interactive")) {
            client = new InteractiveClient(address, port, res.getString("2PLMode"));
        }
        else {
            client = new StoredProcedureClient(address, String.valueOf(port), "Listings,Items,Players".split(","), res.getString("2PLMode"));
        }
        client.setServerConfig(serverConfig);

        long sendingStart = System.currentTimeMillis();

//            TODO: Refactor this function
        long startToWaitTime = System.currentTimeMillis();
        Stats stats = new Stats(5000, 1000, resultFilePath, metricsFilePath, recordSize, 0, 0.0, 0, 20000, benchmarkMode.equals("store") ? res.getString("hotPlayers") : String.valueOf(NUM_OF_HOT_WAREHOUSE), HOT_RECORD_SELECTION_CHANCE, MAX_THREADS, res.getString("2PLMode"), operationDelay);

        Integer numberOfItemsToRead = res.get("readItemNumber");
        Thread itemThread = null;
        if (numberOfItemsToRead > 0 && benchmarkMode.equals("store")) {
            if (res.getInt("maxItemsThreads") != null ) {
                this.MAX_ITEM_READ_THREADS = res.getInt("maxItemsThreads");
                this.MAX_THREADS -= this.MAX_ITEM_READ_THREADS;
            }
            itemThread = runItemThread(sendingStart, benchmarkTime, numberOfItemsToRead, stats);
        }

        for (long i = 0; i < numRecords; i++) {


            long sendStartMs = System.currentTimeMillis();
            long timeElapsed = (sendStartMs - sendingStart) / 1000;

            if (timeElapsed > warmup)
                stats.exitWarmup();

            stats.report(sendStartMs);

            ServerRequest request = getNextRequest(benchmarkMode);
            executeRequestFromThreadPool(request, stats);
//            sendRequestAsync(stats, recordSize, request, c, warmup, maxRetry, timeout, partitionId);


            if (benchmarkTime != null) {
                if (timeElapsed >= benchmarkTime)
                    break;
            }


        }




        // wait for retries to be done
        Thread.sleep(1000 * 3L);
        log.info("Benchmark is finished...");

        // TODO: closing open things?
        /* print final results */
        stats.printTotal();
        metricServer.close();

        if (itemExecutor != null)
            itemExecutor.shutdownNow();
        if (itemThread != null) {
            itemThread.interrupt();
        }
        executor.shutdownNow();


    }

    private void populateHotWarehouses() {

        List<Integer> allWarehouses = new ArrayList<>();
        for (int i = 1; i <= NUM_OF_WAREHOUSES; i++) {
            allWarehouses.add(i);
        }

        // Shuffle the list to randomize the order
        Collections.shuffle(allWarehouses);

        hotWarehouses = allWarehouses.subList(0, NUM_OF_HOT_WAREHOUSE);
        notHotWarehouses = allWarehouses.subList(NUM_OF_HOT_WAREHOUSE, allWarehouses.size());
    }

    private Thread runItemThread(long sendingStart, Long benchmarkTime, Integer numberOfItemsToRead, Stats stats) {
        Thread itemThread;
        itemExecutor = (ThreadPoolExecutor) Executors.newFixedThreadPool(MAX_ITEM_READ_THREADS);
        itemThread = Executors.defaultThreadFactory().newThread(() -> {
            log.info("Item read thread has been created");

            while ((System.currentTimeMillis() - sendingStart) / 1000 < benchmarkTime) {
                // generate a List that contains the numbers 0 to 9
                List<Integer> indices = IntStream.range(0, hotItems.size()).boxed().collect(Collectors.toList());
                Collections.shuffle(indices);
                List<String> itemsToGet = new ArrayList<>();
                for (int i = 0; i < numberOfItemsToRead; i++) {
                    itemsToGet.add(hotItems.get(indices.get(i)));
                }


                itemExecutor.submit(() -> {
                    int itemId = random.nextInt(hotItems.size());
                    boolean status = client.readItem(itemsToGet);
                    log.info("Item {} read finished", hotItems.get(itemId));
                    if (status)
                        stats.itemReadFinished();
                });


                while (itemExecutor.getQueue().size() >= MAX_QUEUE_SIZE) {
                    if ((System.currentTimeMillis() - sendingStart) / 1000 >= benchmarkTime)
                        break;
//                    try {
//                        log.debug("sleep");
//                        Thread.sleep(1);
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
                }
            }
        });
        itemThread.start();
        return itemThread;
    }

    private void populateHotItems() {
        for (String player : hotPlayersAndItems.keySet()) {
            hotItems.addAll(hotPlayersAndItems.get(player));
        }
    }


    private ServerRequest getNextRequest(String benchmarkMode) {
        if (benchmarkMode.equals("store")) {
            return getStoreServerRequest();
        }
        else if (benchmarkMode.equals("tpcc")) {
            return getTPCCServerRequest();
        }
        throw new RuntimeException("Benchmark mode is not defined: " + benchmarkMode);
    }

    private TPCCServerRequest getTPCCServerRequest() {
        int txChance = random.nextInt(100); // Generates a random number between 0 (inclusive) and 100 (exclusive)
        int hotnessChance = random.nextInt(100);
        int warehouseId;
        if (hotnessChance < HOT_RECORD_SELECTION_CHANCE)
            warehouseId = hotWarehouses.get(random.nextInt(hotWarehouses.size()));
        else
            warehouseId = notHotWarehouses.get(random.nextInt(notHotWarehouses.size()));



        if (txChance < 50) {
            return geTPCCPaymentRandomRequest(warehouseId);
        }
        else {
            return getTPCCNewOrderRequest(warehouseId);
        }
    }

    private TPCCServerRequest getTPCCNewOrderRequest(int warehouseId) {
        int districtID = TPCCUtil.randomNumber(1, 10,  random);
        int customerID = TPCCUtil.getCustomerID( random);

        int numItems = TPCCUtil.randomNumber(5, 15,  random);
        int[] itemIDs = new int[numItems];
        int[] supplierWarehouseIDs = new int[numItems];
        int[] orderQuantities = new int[numItems];
        int allLocal = 1;

        for (int i = 0; i < numItems; i++) {
            itemIDs[i] = TPCCUtil.getItemID( random);
            if (TPCCUtil.randomNumber(1, 100,  random) > 1) {
                supplierWarehouseIDs[i] = warehouseId;
            } else {
                do {
                    supplierWarehouseIDs[i] = TPCCUtil.randomNumber(1, NUM_OF_WAREHOUSES,  random);
                } while (supplierWarehouseIDs[i] == warehouseId && NUM_OF_WAREHOUSES > 1);
                allLocal = 0;
            }
            orderQuantities[i] = TPCCUtil.randomNumber(1, 10,  random);
        }
        boolean userAbort = false;

        // we need to cause 1% of the new orders to be rolled back.
        if (TPCCUtil.randomNumber(1, 100,  random) == 1) {
            districtID = TPCCConfig.INVALID_DISTRICT_ID;
            userAbort = true;
        }

        Map<String, String> values = Map.of(
                "warehouseId", String.valueOf(warehouseId),
                "districtId", String.valueOf(districtID),
                "customerId", String.valueOf(customerID),
                "orderLineCount", String.valueOf(numItems),
                "allLocals", String.valueOf(allLocal)
        );

        Map<String, int[]> arrayValues = Map.of(
                "itemIds", itemIDs,
                "supplierWarehouseIds", supplierWarehouseIDs,
                "orderQuantities", orderQuantities
        );


        long sendStartMs = System.currentTimeMillis();
        TPCCServerRequest request = new TPCCServerRequest(TPCCServerRequest.Type.NEW_ORDER, txId++, values, sendStartMs, userAbort);
        request.setArrayValues(arrayValues);
        return request;
    }

    private TPCCServerRequest geTPCCPaymentRandomRequest(int warehouseId) {
        int districtId = TPCCUtil.randomNumber(1, 10, random);
        float paymentAmount = (float) (TPCCUtil.randomNumber(100, 500000, random) / 100.0);

        int x = TPCCUtil.randomNumber(1, 100, random);
        int customerDistrictId;
        if (x <= 85) {
            customerDistrictId = districtId;
        } else {
            customerDistrictId = TPCCUtil.randomNumber(1, 10, random);
        }
        int customerWarehouseID;
        if (x <= 85) {
            customerWarehouseID = warehouseId;
        } else {
            do {
                customerWarehouseID = TPCCUtil.randomNumber(1, NUM_OF_WAREHOUSES, random);
            } while (customerWarehouseID == warehouseId && NUM_OF_WAREHOUSES > 1);
        }
        int customerId = TPCCUtil.getCustomerID(random);

        Map<String, String> values = Map.of(
                "districtId", String.valueOf(districtId),
                "warehouseId", String.valueOf(warehouseId),
                "paymentAmount", String.valueOf(paymentAmount),
                "customerDistrictId", String.valueOf(customerDistrictId),
                "customerWarehouseId", String.valueOf(customerWarehouseID),
                "customerId", String.valueOf(customerId)
        );

        long sendStartMs = System.currentTimeMillis();
        return new TPCCServerRequest(TPCCServerRequest.Type.PAYMENT, txId++, values, sendStartMs, false);
    }

    private StoreServerRequest getStoreServerRequest() {
        //        TODO: Change to correct tx selection
        int chance = random.nextInt(100); // Generates a random number between 0 (inclusive) and 100 (exclusive)
        buy_or_sell++;
        if (chance < HOT_RECORD_SELECTION_CHANCE) {
            buy_or_sell_hot++;
            List<String> playersAsList = new ArrayList<>(hotPlayersAndItems.keySet());
            List<String> playersWhoCanSell = playersAsList.stream().filter(record -> !hotPlayersAndItems.get(record).isEmpty()).toList();

            if (hotListings.isEmpty() && playersWhoCanSell.isEmpty())
                return null;

            if (hotListings.isEmpty())
                buy_or_sell_hot = 1; // FORCE SELL
            else if (playersWhoCanSell.isEmpty())
                buy_or_sell_hot = 2; // FORCE BUY

            if (buy_or_sell_hot % 2 == 0) {
                //buy
                Map<String, String> tx = new HashMap<>();
                String randomPlayer = playersAsList.get(random.nextInt(playersAsList.size()));

                List<String> listingsAsList = new ArrayList<>(hotListings.keySet());
                String randomListing = listingsAsList.get(random.nextInt(listingsAsList.size()));
                // Get the list associated with this random key
//                List<String> randomValues = hotPlayersAndItems.get(randomKey);
                tx.put("PId", randomPlayer);
                tx.put("LId", randomListing);
                tx.put("IId", hotListings.get(randomListing).get(0));
                tx.put("price", hotListings.get(randomListing).get(1));
                hotListings.remove(randomListing);
                long sendStartMs = System.currentTimeMillis();
                return new StoreServerRequest(StoreServerRequest.Type.BUY_HOT, txId++, tx, sendStartMs);
            } else {
                //sell
                Map<String, String> tx = new HashMap<>();
                String randomPlayer = playersWhoCanSell.get(random.nextInt(playersWhoCanSell.size()));

                List<String> randomValues = new ArrayList<>(hotPlayersAndItems.get(randomPlayer));
                String randomItem = randomValues.get(random.nextInt(randomValues.size()));

                tx.put("PId", randomPlayer);
                tx.put("IId", randomItem);
                hotPlayersAndItems.get(randomPlayer).remove(randomItem);
                long sendStartMs = System.currentTimeMillis();
                return new StoreServerRequest(StoreServerRequest.Type.SELL_HOT, txId++, tx, sendStartMs);
            }
        } else {
            Map<String, String> tx = new HashMap<>();
            if (buy_or_sell % 2 == 0) {
                String lid = String.valueOf(++nextNotHotListingID);
                while (alreadyBoughtListing.contains(lid) || hotListings.containsKey(lid))
                    lid = String.valueOf(++nextNotHotListingID);

                alreadyBoughtListing.add(lid);
                tx.put("PId", Integer.toString(random.nextInt(1, NUM_OF_PLAYERS)));
                tx.put("LId", lid);
                long sendStartMs = System.currentTimeMillis();
                return new StoreServerRequest(StoreServerRequest.Type.BUY, txId++, tx, sendStartMs);
            } else {
                String randomPlayer = Integer.toString((++nextNotHotItem / 5));
                while (hotPlayersAndItems.containsKey(randomPlayer))
                    randomPlayer = Integer.toString((++nextNotHotItem / 5));
                String randomItem = Integer.toString(nextNotHotItem);

                tx.put("PId", randomPlayer);
                tx.put("IId", randomItem);
                long sendStartMs = System.currentTimeMillis();
                return new StoreServerRequest(StoreServerRequest.Type.SELL, txId++, tx, sendStartMs);
            }
        }
    }

    private void executeRequestFromThreadPool(ServerRequest request, Stats stats) {
        stats.nextAdded(1);
        log.info("submitting the request {}", request.getValues());
        submitRequest(request, stats);
        while (executor.getQueue().size() >= MAX_QUEUE_SIZE) {
//            try {
//                log.debug("sleep");
////                Thread.sleep(1);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
        }
    }

    private void submitRequest(ServerRequest request, Stats stats) {
        if (request instanceof StoreServerRequest)
            submitRequest((StoreServerRequest) request, stats);
        else if (request instanceof TPCCServerRequest)
            submitRequest((TPCCServerRequest) request, stats);
    }

    private void submitRequest(TPCCServerRequest request, Stats stats) {
        Future<Void> future = executor.submit(()->{
            InteractiveClient.TransactionResult result = new InteractiveClient.TransactionResult();
            if (request.getType() == TPCCServerRequest.Type.PAYMENT) {
                Map<String, String> tx = request.getValues();
                result = client.TPCC_payment(tx.get("warehouseId"),
                        tx.get("districtId"),
                        Float.parseFloat(tx.get("paymentAmount")),
                        tx.get("customerWarehouseId"),
                        tx.get("customerDistrictId"),
                        tx.get("customerId"));
                if (!result.isSuccess()) {
                    log.error("Unsuccessful TPCC Payment {}", tx);
                    stats.addWaistedTime(result.getStart());
                    submitRetry(request, stats);
                }
            }
            else if (request.getType() == TPCCServerRequest.Type.NEW_ORDER) {
                result = client.TPCC_newOrder(request.getValues().get("warehouseId"),
                       request.getValues().get("districtId"),
                       request.getValues().get("customerId"),
                       request.getValues().get("orderLineCount"),
                       request.getValues().get("allLocals"),
                       request.getArrayValues().get("itemIds"),
                       request.getArrayValues().get("supplierWarehouseIds"),
                       request.getArrayValues().get("orderQuantities"));

                if (!result.isSuccess()) {
                    if (!request.isUserAbort()) {
                        log.error("Unsuccessful TPCC new order {},{}", request.getValues(), request.getArrayValues());
                        stats.addWaistedTime(result.getStart());
                        submitRetry(request, stats);
                    }
                    else {
                        log.error("User Abort TPCC new order {},{}", request.getValues(), request.getArrayValues());
                    }
                }
            }

            if (result.isSuccess()) {
                stats.nextCompletion(request.start, 1);
                stats.addAblationTimes(result.getStart(),
                        Long.parseLong(result.getMetrics().get("waiting_time")),
                        Long.parseLong(result.getMetrics().get("io_time")),
                        Long.parseLong(result.getMetrics().get("locking_time")),
                        Long.parseLong(result.getMetrics().get("retiring_time")), Long.parseLong(result.getMetrics().get("initiation_time")), Long.parseLong(result.getMetrics().get("unlocking_time")), Long.parseLong(result.getMetrics().get("committing_time")), Long.parseLong(result.getMetrics().get("waiting_for_others_time")));
                log.info("request successful {}:{}", request.getType(),request.getValues());
            }
            return null;
        });

    }

    private void submitRequest(StoreServerRequest request, Stats stats) {
        Future<Void> future = executor.submit(() -> {
            InteractiveClient.TransactionResult result = new InteractiveClient.TransactionResult();

            if (request.getType() == StoreServerRequest.Type.BUY) {
                result = client.buyListing(request.getValues().get("PId"), request.getValues().get("LId"));
                if (!result.isSuccess()) {
                    log.error("Unsuccessful buy {}", request.getValues());
                    stats.addWaistedTime(result.getStart());
                    // we do not retry records that are not hot!
//                    submitRetry(request, stats);
                }
            }
            else if (request.getType() == StoreServerRequest.Type.SELL) {
                result = client.addListing(request.getValues().get("PId"), request.getValues().get("IId"), 1);
                if (!result.isSuccess()) {
                    log.error("Unsuccessful sell {}", request.getValues());
                    stats.addWaistedTime(result.getStart());
                    // we do not retry records that are not hot!
//                    submitRetry(request, stats);
                }
            }
            else if (request.getType() == StoreServerRequest.Type.BUY_HOT) {
                result = client.buyListing(request.getValues().get("PId"), request.getValues().get("LId"));
                if (result.isSuccess()) {
                    hotPlayersAndItems.get(request.getValues().get("PId")).add(result.getMessage());
                }
                else {
                    stats.addWaistedTime(result.getStart());
                    if (!isGoingToRetry(request))
                        hotListings.put(request.getValues().get("LId"), List.of(request.getValues().get("IId"), request.getValues().get("price")));
                    log.error("Unsuccessful buy {}", request.getValues());
                    submitRetry(request, stats);
                }
            }
            else if (request.getType() == StoreServerRequest.Type.SELL_HOT) {
                result = client.addListing(request.getValues().get("PId"), request.getValues().get("IId"), 1);
                if (result.isSuccess()) {
                    hotListings.put(result.getMessage(), List.of(request.getValues().get("IId"), "1"));
                }
                else {
                    stats.addWaistedTime(result.getStart());
                    if (!isGoingToRetry(request))
                        hotPlayersAndItems.get(request.getValues().get("PId")).add(request.getValues().get("IId"));
                    log.error("Unsuccessful sell {}", request.getValues());
                    submitRetry(request, stats);
                }
            }
            if (result.isSuccess()) {

                stats.nextCompletion(request.start, 1);
                stats.addAblationTimes(result.getStart(),
                        Long.parseLong(result.getMetrics().get("waiting_time")),
                        Long.parseLong(result.getMetrics().get("io_time")),
                        Long.parseLong(result.getMetrics().get("locking_time")),
                        Long.parseLong(result.getMetrics().get("retiring_time")),
                        Long.parseLong(result.getMetrics().get("initiation_time")),
                        Long.parseLong(result.getMetrics().get("unlocking_time")),
                        Long.parseLong(result.getMetrics().get("committing_time")),
                        Long.parseLong(result.getMetrics().get("waiting_for_others_time")));
                log.info("request successful {}", request.getValues());
            }
            return null;
        });
    }

    private boolean isGoingToRetry(ServerRequest request) {
        return MAX_RETRY > 0 && request.getRetried() < MAX_RETRY;
    }

    private void submitRetry(ServerRequest request, Stats stats) {
        if (isGoingToRetry(request)) {
            request.retry();
            stats.addRetry(request.getTransactionId());
            log.error("retrying the request {}, number of retried: {}", request.getValues(), request.getRetried());
            submitRequest(request, stats);
        }
    }

//    private void sendRequestAsync(Stats stats, serverRequest serverRequest, int maxRetry, long timeout) throws  InterruptedException {
//
//        StreamObserver<Result> observer = new StreamObserver<>() {
//            @Override
//            public void onNext(Result result) {
//            }
//
//            @Override
//            public void onError(Throwable throwable) {
//                 serverRequest.retry();
//                if (serverRequest.getRetried() < maxRetry) {
//                    try {
//                        log.info("Retrying batch request {}, number of retried: {}", serverRequest.getBatchId(), serverRequest.getRetried());
//                        stats.addRetry(serverRequest.batchId);
//                        sendRequestAsync(stats, recordSize, serverRequest, c, warmup, maxRetry, timeout, partitionId);
//                    } catch (ExecutionException e) {
//                        throw new RuntimeException(e);
//                    } catch (InterruptedException e) {
//                        throw new RuntimeException(e);
//                    } catch (TimeoutException e) {
//                        throw new RuntimeException(e);
//                    }
//                }
//                else {
//                    log.info("batch request {} reached max retry", serverRequest.getBatchId());
//                }
//            }
//
//            @Override
//            public void onCompleted() {
//                log.info("Batch request {} finished", serverRequest.getBatchId());
//                stats.completeRetry(serverRequest.getBatchId());
////            log.info("Completed batch request {}", batchRequest.getBatchId());
//                long end = System.currentTimeMillis();
////                for (int j = 0; j < batchRequest.getStarts().size() ; j++) {
////                    if (c + j > warmup)
////                        stats.nextCompletion(batchRequest.getStarts().get(j), end, recordSize + 5);
////                }
//                stats.nextBatchCompletion(serverRequest.getValues().size(), serverRequest.getStarts().get(0), end, (recordSize + 5) * serverRequest.getValues().size());
//                log.info("Batch request {} recorded", serverRequest.getBatchId());
//            }
//        };
//
//
//        log.info("Sending Request {} with size {}", serverRequest.getBatchId(), (recordSize + 5) * serverRequest.getValues().size() / (1000 * 1000));
//        stats.nextAdded((recordSize + 5) * serverRequest.getValues().size());
//        asyncDatastore.withDeadlineAfter(timeout, TimeUnit.MILLISECONDS).batch(Values.newBuilder()
//                .setId(serverRequest.batchId)
//                .setPartitionId(partitionId)
//                .putAllValues(serverRequest.getValues())
//                .build()
//        , observer);
//
//        log.info("batch request {} submitted", serverRequest.getBatchId());
//    }

    static byte[] generateRandomPayload(Integer recordSize, List<byte[]> payloadByteList, byte[] payload,
                                        Random random) {
        if (!payloadByteList.isEmpty()) {
            payload = payloadByteList.get(random.nextInt(payloadByteList.size()));
        } else if (recordSize != null) {
            for (int j = 0; j < payload.length; ++j)
                payload[j] = (byte) (random.nextInt(26) + 65);
        } else {
            throw new IllegalArgumentException("no payload File Path or record Size provided");
        }
        return payload;
    }


    static List<byte[]> readPayloadFile(String payloadFilePath, String payloadDelimiter) throws IOException {
        List<byte[]> payloadByteList = new ArrayList<>();
        if (payloadFilePath != null) {
            Path path = Paths.get(payloadFilePath);
            log.info("Reading payloads from: " + path.toAbsolutePath());
            if (Files.notExists(path) || Files.size(path) == 0)  {
                throw new IllegalArgumentException("File does not exist or empty file provided.");
            }

            String[] payloadList = new String(Files.readAllBytes(path), StandardCharsets.UTF_8).split(payloadDelimiter);

            log.info("Number of messages read: " + payloadList.length);

            for (String payload : payloadList) {
                payloadByteList.add(payload.getBytes(StandardCharsets.UTF_8));
            }
        }
        return payloadByteList;
    }

    /** Get the command-line argument parser. */
    static ArgumentParser argParser() {
        ArgumentParser parser = ArgumentParsers
                .newArgumentParser("producer-performance")
                .defaultHelp(true)
                .description("This tool is used to verify the producer performance.");

//        MutuallyExclusiveGroup payloadOptions = parser
//                .addMutuallyExclusiveGroup()
//                .required(true)
//                .description("either --record-size or --payload-file must be specified but not both.");

        MutuallyExclusiveGroup numberOptions = parser
                .addMutuallyExclusiveGroup()
                .required(true)
                .description("either --num-records or --benchmark-time must be specified but not both.");

        parser.addArgument("--address")
                .action(store())
                .required(true)
                .type(String.class)
                .metavar("ADDRESS")
                .dest("address")
                .help("Server address for interactive client / Postgres address for stored procedure / Rocksdb directory");

        parser.addArgument("--port")
                .action(store())
                .required(true)
                .type(Integer.class)
                .metavar("PORT")
                .dest("port")
                .help("Server port for interactive client / Postgres port for stored procedure / -1 For rocksdb");

        numberOptions.addArgument("--num-records")
                .action(store())
                .required(false)
                .type(Long.class)
                .metavar("NUM-RECORDS")
                .dest("numRecords")
                .help("number of messages to produce");

        numberOptions.addArgument("--benchmark-time")
                .action(store())
                .required(false)
                .type(Long.class)
                .metavar("BENCHMARK-TIME")
                .dest("benchmarkTime")
                .help("benchmark time in seconds");

//        payloadOptions.addArgument("--record-size")
//                .action(store())
//                .required(false)
//                .type(Integer.class)
//                .metavar("RECORD-SIZE")
//                .dest("recordSize")
//                .help("message size in bytes. Note that you must provide exactly one of --record-size or --payload-file.");

//        payloadOptions.addArgument("--payload-file")
//                .action(store())
//                .required(false)
//                .type(String.class)
//                .metavar("PAYLOAD-FILE")
//                .dest("payloadFile")
//                .help("file to read the message payloads from. This works only for UTF-8 encoded text files. " +
//                        "Payloads will be read from this file and a payload will be randomly selected when sending messages. " +
//                        "Note that you must provide exactly one of --record-size or --payload-file.");

        parser.addArgument("--result-file")
                .action(store())
                .required(false)
                .type(String.class)
                .metavar("RESULT-FILE")
                .dest("resultFile")
                .help("a csv file containing the total result of benchmark");

        parser.addArgument("--metric-file")
                .action(store())
                .required(false)
                .type(String.class)
                .metavar("METRIC-FILE")
                .dest("metricsFile")
                .help("a csv file containing the timeline result of benchmark");

        parser.addArgument("--partition-id")
                .action(store())
                .required(false)
                .setDefault("")
                .type(String.class)
                .metavar("PARTITION-ID")
                .dest("partitionId")
                .help("Id of the partition that you want to put load on");

        parser.addArgument("--batch-size")
                .action(store())
                .required(false)
                .type(Integer.class)
                .metavar("BATCH-SIZE")
                .dest("batchSize")
                .help("batch size in bytes. This producer batches records in this size and send them to kv store");

        parser.addArgument("--payload-delimiter")
                .action(store())
                .required(false)
                .type(String.class)
                .metavar("PAYLOAD-DELIMITER")
                .dest("payloadDelimiter")
                .setDefault("\\n")
                .help("provides delimiter to be used when --payload-file is provided. " +
                        "Defaults to new line. " +
                        "Note that this parameter will be ignored if --payload-file is not provided.");

        parser.addArgument("--throughput")
                .action(store())
                .required(true)
                .type(Integer.class)
                .metavar("THROUGHPUT")
                .help("throttle maximum message throughput to *approximately* THROUGHPUT messages/sec. Set this to -1 to disable throttling.");

        parser.addArgument("--interval")
                .action(store())
                .required(false)
                .type(Double.class)
                .dest("interval")
                .metavar("INTERVAL")
                .help("interval between each packet.  Set this -1 to send packets blocking");

        parser.addArgument("--timeout")
                .action(store())
                .required(false)
                .type(Integer.class)
                .dest("timeout")
                .metavar("TIMEOUT")
                .help("timeout of each batch request. It is two times of interval by default");

        parser.addArgument("--dynamic-batch-size")
                .action(append())
                .required(false)
                .type(Integer.class)
                .dest("dynamicBatchSize")
                .metavar("DYNAMICBATCHSIZE")
                .help("dynamic batch size until a specific time");

        parser.addArgument("--dynamic-batch-time")
                .action(append())
                .required(false)
                .type(Integer.class)
                .dest("dynamicBatchTime")
                .metavar("DYNAMICBATCHTIME")
                .help("deadline for a dynamic batch size");


        parser.addArgument("--dynamic-interval")
                .action(append())
                .required(false)
                .type(Double.class)
                .dest("dynamicInterval")
                .metavar("DYNAMICINTERVAL")
                .help("dynamic interval until a specific time");

        parser.addArgument("--dynamic-interval-time")
                .action(append())
                .required(false)
                .type(Integer.class)
                .dest("dynamicIntervalTime")
                .metavar("DYNAMICINTERVALTIME")
                .help("deadline for a dynamic interval");

        parser.addArgument("--exponential-load")
                .action(store())
                .required(false)
                .type(Boolean.class)
                .dest("exponentialLoad")
                .metavar("EXPONENTIALLOAD")
                .help("requests follow an exponential random distribution with lambda=1000/interval");

        parser.addArgument("--dynamic-memory")
                .action(append())
                .required(false)
                .type(Long.class)
                .dest("dynamicMemory")
                .metavar("MEMORYTRIGGER")
                .help("trigger the memory after trigger time to this amount");

        parser.addArgument("--dynamic-memory-time")
                .action(append())
                .required(false)
                .type(Integer.class)
                .dest("dynamicMemoryTime")
                .metavar("MEMORYTRIGGERTIME")
                .help("time of memory trigger");


        parser.addArgument("--hot-players")
                .action(store())
                .required(true)
                .type(String.class)
                .dest("hotPlayers")
                .metavar("HOTPLAYERS")
                .help("path to hot players");



        parser.addArgument("--hot-listings")
                .action(store())
                .required(true)
                .type(String.class)
                .dest("hotListings")
                .metavar("HOTLISTING")
                .help("path to hot listings");


        parser.addArgument("--hot-selection-prob")
                .action(store())
                .required(true)
                .type(Integer.class)
                .dest("hotSelectionProb")
                .metavar("HOTSELECTION")
                .help("chance of a hot record being selected");


        parser.addArgument("--max-threads")
                .action(store())
                .required(false)
                .type(Integer.class)
                .dest("maxThreads")
                .metavar("MAXTHREADS")
                .help("number of maximum threads for sending the load");


        parser.addArgument("--max-retry")
                .action(store())
                .required(false)
                .setDefault(-1)
                .type(Integer.class)
                .dest("maxRetry")
                .metavar("MAXRETRY")
                .help("Maximum number of times a request can be retried");

        parser.addArgument("--max-items-threads")
                .action(store())
                .required(false)
                .type(Integer.class)
                .dest("maxItemsThreads")
                .metavar("MAXITEMSTHREADS")
                .help("number of maximum threads for reading item");


        parser.addArgument("--read-item-number")
                .action(store())
                .required(true)
                .type(Integer.class)
                .dest("readItemNumber")
                .metavar("READITEMNUMBER")
                .help("number of items to read at the same time");

        parser.addArgument("--operation-delay")
                .action(store())
                .required(false)
                .type(Integer.class)
                .dest("operationDelay")
                .metavar("OPERATIONDELAY")
                .help("the amount of time each operation will be delayed, mimicking the thinking time.");


        parser.addArgument("--2pl-mode")
                .action(store())
                .required(false)
                .setDefault("slw")
                .type(String.class)
                .dest("2PLMode")
                .metavar("2PLMODE")
                .help("2pl algorithm used in server. ww: Wound-Wait, bamboo: Bamboo, slw: SLW-Graph");


        parser.addArgument("--benchmark-mode")
                .action(store())
                .required(false)
                .setDefault("store")
                .type(String.class)
                .dest("benchmarkMode")
                .metavar("BENCHMARK_MODE")
                .help("type of the benchmark used. could be either \"store\" or \"tpcc\".");

        parser.addArgument("--num-of-warehouses")
                .action(store())
                .required(false)
                .setDefault(1)
                .type(Integer.class)
                .dest("numOfWarehouses")
                .metavar("WAREHOUSECOUNT")
                .help("number of warehouses in tpcc benchmark.");

        parser.addArgument("--hot-warehouses")
                .action(store())
                .required(false)
                .setDefault(0)
                .type(Integer.class)
                .dest("hotWarehouses")
                .metavar("HOTWAREHOUSE")
                .help("number of hot warehouse records in tpcc benchmark.");

        parser.addArgument("--transaction-mode")
                .action(store())
                .required(false)
                .setDefault("interactive")
                .type(String.class)
                .dest("transactionMode")
                .metavar("TRANSACTIONMODE")
                .help("Transaction execution mode. could be either \"interactive\" or \"storedProcedure\"");



        return parser;
    }

    private static class Stats {
        private boolean warmup;
        private long start;
        private long windowStart;
        private int[] latencies;
        private int sampling;
        private int iteration;
        private int index;
        private long count;
        private long bytes;
        private int maxLatency;
        private long totalLatency;
        private long windowCount;
        private int windowMaxLatency;
        private long windowTotalLatency;
        private long windowBytes;
        private long reportingInterval;

        private long numRecords;
        private int recordSize;
        private int batchSize;
        private String resultFilePath;
        private double interval;
        private int timeout;
        private Map<Long, Integer> retries;
        private int previousWindowRequestRetried;
        private int previousWindowRetries;
        private int completedRetries;
        private int previousWindowCompletedRetries;

        private String metricsFilePath;
        private int started;
        private int startedBytes;
        private int startedWindowBytes;
        private String hotRecords;
        private int hotChance;

        private int threads;

        private String mode;
        private int operationDelay;

        private long usefulWorkTime;
        private long waitedTime;
        private long ioTime;
        private long lockingTime;
        private long wastedTimeWork;
        private long retiringTime;
        private long initiationTime;
        private long unlockingTime;
        private long committingTime;
        private long waitingForOthersTime;

        // Metrics
        private static final Summary finishedRequestsBytes = Summary.build()
                .name("paxos_requests_finished_bytes")
                .help("size of requests that finished")
                .register();
        private static final Summary addedRequestsBytes = Summary.build()
                .name("paxos_requests_sent_bytes")
                .help("size of requests sent to the leader")
                .register();
        private static final Gauge batchSizeGauge = Gauge.build()
                .name("paxos_batch_size")
                .help("Batch size")
                .register();
        private static final Gauge intervalGauge = Gauge.build()
                .name("paxos_interval_total")
                .help("Interval between requests")
                .register();
        private static final Gauge timeoutGauge = Gauge.build()
                .name("paxos_timeout_total")
                .help("timeout of a request")
                .register();
        private static final Summary finishedRequestsLatency = Summary.build()
                .name("paxos_requests_finished_latency")
                .help("Latency of requests responded")
                .quantile(0.5, 0.001)    // 0.5 quantile (median) with 0.01 allowed error
                .quantile(0.95, 0.005)  // 0.95 quantile with 0.005 allowed error
                .register();
        private static final Counter requestRetried = Counter.build()
                .name("paxos_requests_retired_total")
                .help("Number of requests that retried")
                .register();
        private static final Counter allRetries = Counter.build()
                .name("paxos_request_retries_total")
                .help("Number of request retries")
                .register();
        private static final Counter retriesCompleted = Counter.build()
                .name("paxos_request_retry_completed_total")
                .help("Number of retries that has been compelted")
                .register();

        private static final Gauge memoryGauge = Gauge.build()
                .name("memory_usage")
                .help("Memory usage of database")
                .register();
        private int itemCount;

        public Stats(long numRecords, int reportingInterval, String resultFilePath, String metricsFilePath, int recordSize, int batchSize, Double interval, int timeout, long memory, String hotRecords, int hotChance, int threads, String mode, int operationDelay) {
            init(numRecords, reportingInterval, resultFilePath, metricsFilePath, recordSize, batchSize, interval, timeout, memory, hotRecords, hotChance, threads, mode, operationDelay);
        }

        private void init(long numRecords, int reportingInterval, String resultFilePath, String metricsFilePath, int recordSize, int batchSize, Double interval, int timeout, long memory, String hotRecords, int hotChance, int threads, String mode, int operationDelay) {
            this.start = System.currentTimeMillis();
            this.windowStart = System.currentTimeMillis();
            this.iteration = 0;
            this.sampling = (int) (numRecords / Math.min(numRecords, 500000));
            this.latencies = new int[(int) (numRecords / this.sampling) + 1];
            this.index = 0;
            this.started = 0;
            this.startedBytes = 0;
            this.startedWindowBytes = 0;
            this.maxLatency = 0;
            this.totalLatency = 0;
            this.windowCount = 0;
            this.windowMaxLatency = 0;
            this.windowTotalLatency = 0;
            this.windowBytes = 0;
            this.totalLatency = 0;
            this.previousWindowRequestRetried = 0;
            this.previousWindowRetries = 0;
            this.completedRetries = 0;
            this.previousWindowCompletedRetries = 0;
            this.reportingInterval = reportingInterval;
            this.resultFilePath = resultFilePath;
            this.metricsFilePath = metricsFilePath;
            this.numRecords = numRecords;
            this.recordSize = recordSize;
            this.batchSize = batchSize;
            batchSizeGauge.set(batchSize);
            this.interval = interval;
            intervalGauge.set(interval);
            this.timeout = timeout;
            timeoutGauge.set(timeout);
            memoryGauge.set(memory);
            this.retries = new ConcurrentHashMap<>();
            this.hotRecords = hotRecords;
            this.hotChance = hotChance;
            this.threads = threads;
            createResultCSVFiles(resultFilePath);
            this.operationDelay = operationDelay;
            this.mode = mode;
            this.usefulWorkTime = 0;
            this.waitedTime = 0;
            this.ioTime = 0;
            this.wastedTimeWork = 0;
            this.lockingTime = 0;
            this.retiringTime = 0;
            this.initiationTime = 0;
            this.unlockingTime = 0;
            this.committingTime = 0;
            this.waitingForOthersTime = 0;
        }

        private void createResultCSVFiles(String resultFilePath) {
            try {
                Path path = Paths.get(resultFilePath);
                // Ensure the parent directories exist
                Files.createDirectories(path.getParent());

                // Check if the file already exists to avoid overwriting it
                if (!Files.exists(path)) {
                    String CSVHeader = "num of records, mode, operation_delay, hot_records, prob, threads, throughput(tx/s), item_read(tx/s), request_retried, total_retries, avg_retry_per_request, avg_latency, max_latency, 50th_latency, 95th_latency, 99th_latency, 99.9th_latency, useful_time, waited_time, wasted_time, io_time, locking_time, retiring_time, initiation_time, unlocking_time, committing_time, waiting_for_others_time\n";
                    BufferedWriter out = new BufferedWriter(new FileWriter(resultFilePath));

                    // Writing the header to output stream
                    out.write(CSVHeader);

                    // Closing the stream
                    out.close();
                }
            } catch (IOException ex) {
                System.out.println(ex.getMessage());
                log.warn("Invalid path or error creating the directories");
            }
        }

        public synchronized void record(int iter, int latency, int bytes, long time) {
            this.count++;
            this.bytes += bytes;
            finishedRequestsBytes.observe(bytes);
            finishedRequestsLatency.observe((double) latency/1000);
            this.totalLatency += latency;
            this.maxLatency = Math.max(this.maxLatency, latency);
            this.windowCount++;
            this.windowBytes += bytes;
            this.windowTotalLatency += latency;
            this.windowMaxLatency = Math.max(windowMaxLatency, latency);
            if (iter % this.sampling == 0) {
                this.latencies[index] = latency;
                this.index++;
            }
            report(time);
        }

        private void report(long time) {
            if (warmup)
                return;
            if (time - windowStart >= reportingInterval) {
                printWindow();
                newWindow();
            }
        }

        public void nextCompletion(long start, int bytes) {
            if (warmup)
                return;
            long now = System.currentTimeMillis();
            int latency = (int) (now - start);
            record(iteration, latency, bytes, now);
            this.iteration++;
        }

        public void nextBatchCompletion(long quantity, long start, long end, int bytes) {
            this.count += quantity - 1;
            this.windowCount += quantity - 1;
            nextCompletion(start, end, bytes);
        }

        public synchronized void nextAdded(int bytes) {
            if (warmup)
                return;
            long now = System.currentTimeMillis();
            this.started++;
            addedRequestsBytes.observe(bytes);
            this.startedBytes += bytes;
            this.startedWindowBytes += bytes;

            report(now);
        }

        public void nextCompletion(long start, long end, int bytes) {
            if (warmup)
                return;
            int latency = (int) (end - start);
            record(iteration, latency, bytes, end);
            this.iteration++;
        }

        public void printWindow() {
            long elapsed = System.currentTimeMillis() - windowStart;
            double recsPerSec = 1000.0 * windowCount / (double) elapsed;
            double mbPerSec = 1000.0 * this.windowBytes / (double) elapsed / (1024.0);
            double throughputMbPerSec = 1000.0 * this.startedWindowBytes / (double) elapsed / (1024.0);
            System.out.printf("%d records sent, %.1f records/sec (%.3f KB/sec) of (%.3f KB/sec), %.1f ms avg latency, %.1f ms max latency.%n%n",
                    windowCount,
                    recsPerSec,
                    mbPerSec,
                    throughputMbPerSec,
                    windowTotalLatency / (double) windowCount,
                    (double) windowMaxLatency);

        }

        public void newWindow() {
            this.windowStart = System.currentTimeMillis();
            this.windowCount = 0;
            this.windowMaxLatency = 0;
            this.windowTotalLatency = 0;
            this.windowBytes = 0;
            this.startedWindowBytes = 0;
            previousWindowRetries = retries.size();
            previousWindowRequestRetried = retries.values().stream().mapToInt(Integer::intValue).sum();
            previousWindowCompletedRetries = completedRetries;
        }

        public void printTotal() {
            if (warmup)
                return;
            long elapsed = System.currentTimeMillis() - start;
            double recsPerSec = 1000.0 * count / (double) elapsed;
            double itemsPerSec = 1000.0 * itemCount / (double) elapsed;
            double mbPerSec = 1000.0 * this.bytes / (double) elapsed / (1024.0);
            double throughputMbPerSec = 1000.0 * this.startedBytes / (double) elapsed / (1024.0);
            int[] percs = percentiles(this.latencies, index, 0.5, 0.95, 0.99, 0.999);
            int numOfRetries = retries.size();
            int totalRetries = retries.values().stream().mapToInt(Integer::intValue).sum();
            Double avgRetryPerReq = retries.isEmpty() ? 0.0 : retries.values().stream().mapToInt(Integer::intValue).average().getAsDouble();
            System.out.printf("%d records sent, %f records/sec (%.3f KB/sec) of (%.3f KB/sec), %.3f items/sec, %.2f ms avg latency, %.2f ms max latency, %d ms 50th, %d ms 95th, %d ms 99th, %d ms 99.9th.%n --  requests retried: %d, retries: %d, avg retry per request: %.2f, useful work time: %d, waited time: %d, wasted time: %d, IO time: %d, Locking time: %d, retiringTime: %d, initiationTime: %d, unlockingTime: %d, committingTime: %d, waitingForOthersTime: %d\n",
                    count,
                    recsPerSec,
                    mbPerSec,
                    throughputMbPerSec,
                    itemsPerSec,
                    totalLatency / (double) count,
                    (double) maxLatency,
                    percs[0],
                    percs[1],
                    percs[2],
                    percs[3],
                    numOfRetries,
                    totalRetries,
                    avgRetryPerReq,
                    usefulWorkTime,
                    waitedTime,
                    wastedTimeWork,
                    ioTime,
                    lockingTime,
                    retiringTime,
                    initiationTime,
                    unlockingTime,
                    committingTime,
                    waitingForOthersTime
                    );

            System.out.println("");

            String resultCSV = String.format("%d,%s,%d,%s,%d,%d,%.2f,%.2f,%d,%d,%.2f,%.2f,%.2f,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d\n",
                    count,
                    mode,
                    operationDelay,
                    hotRecords,
                    hotChance,
                    threads,
                    recsPerSec,
                    itemsPerSec,
                    numOfRetries,
                    totalRetries,
                    avgRetryPerReq,
                    totalLatency / (double) count,
                    (double) maxLatency,
                    percs[0],
                    percs[1],
                    percs[2],
                    percs[3],
                    usefulWorkTime,
                    waitedTime,
                    wastedTimeWork,
                    ioTime,
                    lockingTime,
                    retiringTime,
                    initiationTime,
                    unlockingTime,
                    committingTime,
                    waitingForOthersTime);
            try {
                BufferedWriter out = new BufferedWriter(
                        new FileWriter(resultFilePath, true));

                // Writing on output stream
                out.write(resultCSV);
                // Closing the connection
                out.close();
            }
            catch (IOException ex) {
                log.warn("Invalid path");
            }
            log.info(resultCSV);
        }

        private static int[] percentiles(int[] latencies, int count, double... percentiles) {
            int size = Math.min(count, latencies.length);
            Arrays.sort(latencies, 0, size);
            int[] values = new int[percentiles.length];
            for (int i = 0; i < percentiles.length; i++) {
                int index = (int) (percentiles[i] * size);
                values[i] = latencies[index];
            }
            return values;
        }

        public void addRetry(long c) {
            if (warmup)
                return;
            allRetries.inc();
            if (retries.containsKey(c))
                retries.put(c, retries.get(c) + 1);
            else {
                retries.put(c, 1);
                requestRetried.inc();
            }
        }

        public Integer getNumberOfRetries(long c) {
            return retries.getOrDefault(c, 0);
        }

        public void updateBatchSize(Integer batchSize) {
            this.batchSize = batchSize;
            batchSizeGauge.set(batchSize);
        }


        public void updateInterval(Double interval) {
            this.interval = interval;
            intervalGauge.set(interval);
        }

        public void completeRetry(long batchId) {
            if (retries.containsKey(batchId)) {
                retries.remove(batchId);
                retriesCompleted.inc();
                completedRetries++;
            }
        }

        public boolean isThereAnyRetry() {
            return !retries.isEmpty();
        }

        public void changeMemory(long memoryTrigger) {
            memoryGauge.set(memoryTrigger);
        }

        public void exitWarmup() {
            if (warmup) {
                this.start = System.currentTimeMillis();
                warmup = false;
            }
        }

        public void itemReadFinished() {
            if (warmup)
                return;
            this.itemCount++;
        }

        public void addAblationTimes(long start, long waitingTime, long ioTime, long lockingTime, long retiringTime, long initiationTime, long unlockingTime, long committingTime, long waitingForOthersTime) {
            long now = System.currentTimeMillis();
            this.usefulWorkTime += ((now - start) - waitingTime);
            this.waitedTime += waitingTime;
            this.ioTime += ioTime;
            this.lockingTime += lockingTime;
            this.retiringTime += retiringTime;
            this.initiationTime += initiationTime;
            this.unlockingTime += unlockingTime;
            this.committingTime += committingTime;
            this.waitingForOthersTime += waitingForOthersTime;
        }

        public void addWaistedTime(long start) {
            long now = System.currentTimeMillis();
            this.wastedTimeWork += (now - start);
        }
    }



}