package edgelab.retryFreeDB.repo.storage.DTO;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Map;

@EqualsAndHashCode(callSuper = true)
@Data
public class DBWriteData extends DBData {
    private String variable;
    private String value;
    private Map<String,String> multiValue;
}
