package edgelab.retryFreeDB.repo.storage;

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
import java.nio.file.Files;
import java.util.Map;

@Slf4j
public class RocksDBRepo implements KeyValueRepository{

//    TODO: SHould implement rollback mechanism


    @Override
    public void insert(DBTransaction tx, DBInsertData data) throws Exception {

    }

    @Override
    public void remove(DBTransaction tx, DBDeleteData data) throws Exception {

    }

    @Override
    public void write(DBTransaction tx, DBWriteData data) throws Exception {

    }

    @Override
    public String read(DBTransaction tx, DBData data) throws Exception {
        return null;
    }

    @Override
    public Integer lastId(String table) throws Exception {
        return null;
    }

    @Override
    public void rollback(DBTransaction tx) throws Exception {

    }
}