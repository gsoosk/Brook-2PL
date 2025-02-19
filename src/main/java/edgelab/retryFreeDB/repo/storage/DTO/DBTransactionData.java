package edgelab.retryFreeDB.repo.storage.DTO;

import edgelab.proto.Transaction;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

import static edgelab.retryFreeDB.RetryFreeDBConfiguration.DELETE_TYPE;
import static edgelab.retryFreeDB.RetryFreeDBConfiguration.INSERT_TYPE;
import static edgelab.retryFreeDB.RetryFreeDBConfiguration.WRITE_TYPE;

@Slf4j
public class DBTransactionData {


    @Setter
    @Getter
    private List<DBData> dataList;

    public DBTransactionData(List<DBData> dList) {
        dataList = dList;
    }

    public static DBTransactionData deserialize(Transaction transaction) {
        List<DBData> dList = new ArrayList<>();

        for (edgelab.proto.Data data:
             transaction.getReadWriteList()) {
            DBData d = deserializeData(data);
            if (d != null) {
                dList.add(d);
            }

        }
        return new DBTransactionData(dList);
    }

    public static DBData deserializeData(edgelab.proto.Data data) {
        String type = data.getType();
        DBData d;

        if (type.equals(WRITE_TYPE))
            d = new DBWriteData();
        else if(type.equals(DELETE_TYPE))
            d = new DBDeleteData();
        else if(type.equals(INSERT_TYPE))
            d = new DBInsertData();
        else
            d = new DBData();


        if (!type.equals(INSERT_TYPE)) {
            String[] keys = data.getKey().split(",");
            if (keys.length < 3) {
                log.error("could not deserialize: not enough keys in Key string");
                return null;
            }
            d.setTable(keys[0]);
            List<String> ids = new ArrayList<>();
            List<Integer> queries = new ArrayList<>();
            for (int i = 1; i <= keys.length/2 ; i++) {
                ids.add(keys[i]);
                queries.add(Integer.parseInt(keys[i + keys.length/2]));
            }
            d.setIds(ids);
            d.setQueries(queries);
        }
        else {
            d.setTable(data.getKey());
        }

        if (d instanceof DBWriteData) {
            if (!data.getValue().equals(null) && !data.getValue().equals("")) {
                String[] writeKeys = data.getValue().split(",");
                ((DBWriteData) d).setVariable(writeKeys[0]);
                ((DBWriteData) d).setValue(writeKeys[1]);
            }
        }
        else if (d instanceof DBInsertData) {
            ((DBInsertData) d).setNewRecord(data.getValue());
            ((DBInsertData) d).setRecordId(data.getRecordId());
            ((DBInsertData) d).setRecordKeys(data.getRecordKeys());
        }


        return d;
    }


}
