package edgelab.retryFreeDB.repo.storage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edgelab.retryFreeDB.init.InitStoreBenchmark;
import edgelab.retryFreeDB.repo.concurrencyControl.DBTransaction;
import edgelab.retryFreeDB.repo.storage.DTO.DBData;
import edgelab.retryFreeDB.repo.storage.DTO.DBDeleteData;
import edgelab.retryFreeDB.repo.storage.DTO.DBInsertData;
import edgelab.retryFreeDB.repo.storage.DTO.DBWriteData;
import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.rocksdb.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slf4j
public class RocksDBRepo implements KeyValueRepository{

//    TODO: SHould implement rollback mechanism with WAL
//    TODO: move dirty read to postgres


//    private RocksDB rocksDB;
    private Map<String, Map<String, String>> rocksDB = new ConcurrentHashMap<>();
    private static final ObjectMapper objectMapper = new ObjectMapper();
    public RocksDBRepo(String dbDirectory) throws RocksDBException {


        try (Options options = new Options().setCreateIfMissing(false);
             RocksDB db = RocksDB.open(options, dbDirectory)) {
             RocksIterator iterator = db.newIterator();
             ExecutorService executor = Executors.newFixedThreadPool(32);

            iterator.seekToFirst();
            while (iterator.isValid()) {
                byte[] keyBytes = iterator.key();
                byte[] valueBytes = iterator.value();
                iterator.next();

                executor.execute(() -> {
                    try {
                        String key = new String(keyBytes, StandardCharsets.UTF_8);
                        String value = new String(valueBytes, StandardCharsets.UTF_8);
                        rocksDB.put(key, objectMapper.readValue(value, Map.class));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
                }

                executor.shutdown();
                executor.awaitTermination(10, TimeUnit.MINUTES);
            } catch (InterruptedException ex) {
            throw new RuntimeException(ex);
        }

        System.out.println("Loaded " + rocksDB.size() + " entries into HashMap");



//        DBInit(dbDirectory);
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


        Map<String, String> value = new HashMap<>();
        String[] record = data.getNewRecord().split(",");
        String[] keys = data.getRecordKeys().split(",");

        for (int i = 0 ; i < keys.length ; i++) {
            value.put(keys[i], record[i]);
        }


        rocksDB.put(key, value);
        tx.appendWAL(key, null);

    }

    @Override
    public void remove(DBTransaction tx, DBDeleteData data) throws Exception {
//        try {
            log.info("{}: remove {}:{}",tx.getTimestamp(), data.getTable(), data.getIds());
            String key = getKey(data);

            Map<String, String> initValue = rocksDB.remove(key);
            if (initValue != null)
                tx.appendWAL(key, initValue);

    }

    @Override
    public void write(DBTransaction tx, DBWriteData data) throws Exception {
        try {
            log.info("{}: update {}:{}",tx.getTimestamp(), data.getTable(), data.getIds());
            String key = getKey(data);

            /* There is a logical lock on the key*/
            Map<String, String> value = rocksDB.get(key);
            if (value == null)
                throw new RocksDBException("not existed");
            tx.appendWAL(key, value);

            if (!value.containsKey(data.getVariable()))
                throw new RocksDBException("does not contain variable: " + data.getVariable());

            value.put(data.getVariable(), data.getValue());
            rocksDB.put(key, value);
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
    public String read(DBTransaction tx, DBData data) throws Exception {
        try {
            log.info("{}: get {}:{}",tx.getTimestamp(), data.getTable(), data.getIds());
            String dbKey = getKey(data);
            Map<String, String> mapData = rocksDB.get(dbKey);
            if (mapData == null)
                throw new RocksDBException("not existed");


            StringBuilder value = new StringBuilder();
            for (String key : mapData.keySet()) {
                value.append(key.toLowerCase()).append(":");
                value.append(mapData.get(key));

                value.append(",");
            }

            value.delete(value.length()-1, value.length());
            return value.toString();
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
                rocksDB.put(key, wal.get(key).get());
            else
                rocksDB.remove(key);
        }
    }

    @Override
    public void commit(DBTransaction tx) throws Exception {

    }
}