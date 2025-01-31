package edgelab.retryFreeDB.repo.storage.DTO;

import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper = true)
@Data
public class DBInsertData extends DBData{
    private String recordId;
    private String recordKeys;
    private String newRecord;
}
