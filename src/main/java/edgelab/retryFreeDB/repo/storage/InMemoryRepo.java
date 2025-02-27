package edgelab.retryFreeDB.repo.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import edgelab.retryFreeDB.init.InitStoreBenchmark;
import edgelab.retryFreeDB.init.InitTPCCBenchmark;
import edgelab.retryFreeDB.repo.concurrencyControl.DBTransaction;
import edgelab.retryFreeDB.repo.storage.DTO.DBData;
import edgelab.retryFreeDB.repo.storage.DTO.DBDeleteData;
import edgelab.retryFreeDB.repo.storage.DTO.DBInsertData;
import edgelab.retryFreeDB.repo.storage.DTO.DBWriteData;
import lombok.extern.slf4j.Slf4j;
import org.jgrapht.alg.util.Pair;
import org.rocksdb.*;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@Slf4j
public class InMemoryRepo implements KeyValueRepository{

//    TODO: SHould implement rollback mechanism with WAL
//    TODO: move dirty read to postgres


    private final Map<String, Map<String, String>> inMemoryDB;
    private static final ObjectMapper objectMapper = new ObjectMapper();
    public InMemoryRepo(String dbDirectory) throws RocksDBException {
        {
            try (Options options = new Options().setCreateIfMissing(false);
                 RocksDB db = RocksDB.open(options, dbDirectory)) {

                RocksIterator iterator = db.newIterator();
                int numThreads = Runtime.getRuntime().availableProcessors();
                ExecutorService executor = Executors.newFixedThreadPool(numThreads);
                List<List<Pair<byte[], byte[]>>> partitions = new ArrayList<>();
                for (int i = 0; i < numThreads; i++) {
                    partitions.add(new ArrayList<>());
                }

                int index = 0;
                iterator.seekToFirst();
                while (iterator.isValid()) {
                    byte[] keyBytes = iterator.key();
                    byte[] valueBytes = iterator.value();
                    partitions.get(index % numThreads).add(new Pair<>(keyBytes, valueBytes));
                    iterator.next();
                    index++;
                }

                System.out.println("Have read " + index + " entries from db");
                inMemoryDB = new ConcurrentHashMap<>(index*4/3, 0.75f, Runtime.getRuntime().availableProcessors() * 2);
                List<Future<Void>> futures = new ArrayList<>();
                for (List<Pair<byte[], byte[]>> partition : partitions) {
                    futures.add(executor.submit(() -> {
                        for (Pair<byte[], byte[]> entry : partition) {
                            try {
                                String key = new String(entry.getFirst(), StandardCharsets.UTF_8);
                                Map<String, String> value = objectMapper.readValue(entry.getSecond(), Map.class);
                                inMemoryDB.put(key, value);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                        return null;
                    }));
                }

                for (Future<Void> future : futures) {
                    future.get(); // Ensure all tasks complete
                }

                executor.shutdown();
                executor.awaitTermination(10, TimeUnit.MINUTES);
            } catch (InterruptedException | ExecutionException ex) {
                throw new RuntimeException(ex);
            }
//            inMemoryDB = InitTPCCBenchmark.loadToMemory(32);
        }
        System.gc();

        System.out.println("Loaded " + inMemoryDB.size() + " entries into HashMap");
    }

    public int getDBSize() {
        return inMemoryDB.size();
    }

//    private void DBInit(String dbPath) throws RocksDBException {
//        RocksDB.loadLibrary();
//        final Options options = new Options();
//        options.setCreateIfMissing(true)
//                .setIncreaseParallelism(32)
//                .setMaxBackgroundJobs(32)  // Tune based on available CPU cores
//                .setAllowMmapReads(true)  // For faster reads
//                .setAllowMmapWrites(true) // For faster writes
//                .setCompressionType(CompressionType.LZ4_COMPRESSION) // Balanced performance
//                .setWriteBufferSize(60L * 1024 * 1024 * 1024) // Larger buffer for batch writes
//                .setMaxWriteBufferNumber(100)
//                .setMinWriteBufferNumberToMerge(2);
//
////                .setUseFsync(false)
////                .setAvoidFlushDuringShutdown(true) // Avoid flushing data on shutdown
////                .setAvoidFlushDuringRecovery(true); // Skip flushing during recovery;
//
//        try {
//            rocksDB = RocksDB.open(options, dbPath);
//            log.info("RocksDB initialized and ready to use");
//        } catch (RocksDBException ex) {
//            log.error("Could not init RocksDB");
//            throw ex;
//        }
//    }

    @Override
    public void insert(DBTransaction tx, DBInsertData data) throws Exception {
        log.info("{}: insert {}:{}",tx.getTimestamp(), data.getTable(), data.getRecordId());
        String key = data.getTable() + ":" + data.getRecordId();

        Map<String, String> value;
        if (data.getNewRecordData() != null && !data.getNewRecordData().isEmpty()) {
            value = data.getNewRecordData();
        } else {
            value = new HashMap<>();
            String[] record = data.getNewRecord().split(",");
            String[] keys = data.getRecordKeys().split(",");

            for (int i = 0; i < keys.length; i++) {
                value.put(keys[i], record[i]);
            }
        }

        if (!data.getTable().equals("order_line") && !data.getTable().equals("new_order") && !data.getTable().equals("history") && !data.getTable().equals("oorder")) { // do not save orderline and neworder due to memory limit
            inMemoryDB.put(key, value);
            tx.appendWAL(key, null);
        }
    }

    @Override
    public void remove(DBTransaction tx, DBDeleteData data) throws Exception {
//        try {
            log.info("{}: remove {}:{}",tx.getTimestamp(), data.getTable(), data.getIds());
            String key = getKey(data);

            Map<String, String> initValue = inMemoryDB.remove(key);
            if (initValue != null)
                tx.appendWAL(key, initValue);

    }

    @Override
    public void write(DBTransaction tx, DBWriteData data) throws Exception {
        try {
            log.info("{}: update {}:{}",tx.getTimestamp(), data.getTable(), data.getIds());
            String key = getKey(data);

            /* There is a logical lock on the key*/
            Map<String, String> value = inMemoryDB.get(key);
            if (value == null)
                throw new RocksDBException("not existed");
            tx.appendWAL(key, value);

            if (data.getMultiValue() != null && !data.getMultiValue().isEmpty()) {
                value.putAll(data.getMultiValue());
            }
            else {
                if (!value.containsKey(data.getVariable()))
                    throw new RocksDBException("does not contain variable: " + data.getVariable());
                value.put(data.getVariable(), data.getValue());
            }

            inMemoryDB.put(key, value);
        } catch (RocksDBException e) {
            log.error("{}: Could not write <{},{}>",tx.getTimestamp(), data.getTable() , data.getIds());
            throw e;
        }

    }


    private String getKey(DBData data) {
        StringBuilder key = new StringBuilder();
        key.append(data.getTable()).append(":");
        for (int i = 0; i < data.getQueries().size(); i++) {
            key.append(data.getQueries().get(i));
            if (i < data.getQueries().size() - 1)
                key.append(",");
        }
        return key.toString();
    }

    @Override
    public Map<String, String> read(DBTransaction tx, DBData data) throws Exception {
        try {
            log.info("{}: get {}:{}",tx.getTimestamp(), data.getTable(), data.getIds());
            String dbKey = getKey(data);
            Map<String, String> mapData = inMemoryDB.get(dbKey);
            if (mapData == null)
                throw new RocksDBException("not existed");
            return mapData;
        } catch (RocksDBException e) {
            log.error("Could not read <{}>", getKey(data));
            throw e;
        }
    }

    @Override
    public Integer lastId(String table) throws Exception {
        if (InitStoreBenchmark.lastIdUsed.containsKey(table))
            return InitStoreBenchmark.lastIdUsed.get(table);

        throw new Exception("Could not find table");
    }

    @Override
    public void rollback(DBTransaction tx) throws Exception {
    Map<String, Optional<Map<String, String>>> wal = tx.getWAL();
        for (String key:
             wal.keySet()) {
            if (wal.get(key).isPresent())
                inMemoryDB.put(key, wal.get(key).get());
            else
                inMemoryDB.remove(key);
        }
    }

    @Override
    public void commit(DBTransaction tx) throws Exception {

    }
}