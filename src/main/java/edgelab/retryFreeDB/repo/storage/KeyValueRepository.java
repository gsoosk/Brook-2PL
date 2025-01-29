package edgelab.retryFreeDB.repo.storage;

import edgelab.retryFreeDB.repo.concurrencyControl.DBTransaction;
import edgelab.retryFreeDB.repo.storage.DTO.DBData;
import edgelab.retryFreeDB.repo.storage.DTO.DBDeleteData;
import edgelab.retryFreeDB.repo.storage.DTO.DBInsertData;
import edgelab.retryFreeDB.repo.storage.DTO.DBWriteData;

import java.sql.SQLException;
import java.util.Map;

public interface KeyValueRepository {
    public static  long OPERATION_THINKING_TIME = 0;
    void insert(DBTransaction tx, DBInsertData data) throws Exception;
    void remove(DBTransaction tx, DBDeleteData data) throws Exception;
    void write(DBTransaction tx, DBWriteData data) throws Exception;
    String read(DBTransaction tx, DBData data) throws Exception;
    Integer lastId(String table) throws Exception;
    void rollback(DBTransaction tx) throws Exception;


}