package edgelab.retryFreeDB.repo.storage.DTO;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Map;

@EqualsAndHashCode(callSuper = true)
@Data
public class DBInsertData extends DBData{
    private String recordId;
    private String recordKeys;
    private String newRecord;
    private Map<String, String> newRecordData;
}
