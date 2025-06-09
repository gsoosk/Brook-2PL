package edgelab.retryFreeDB.repo.storage.DTO;

import lombok.Data;

import java.util.List;

@Data
public class DBData {
    private String table;
    private List<String> ids;
    private List<Integer> queries;

    public String getKey() {
        StringBuilder key = new StringBuilder();
        key.append(this.getTable()).append(":");
        for (int i = 0; i < this.getQueries().size(); i++) {
            key.append(this.getQueries().get(i));
            if (i < this.getQueries().size() - 1)
                key.append(",");
        }
        return key.toString();
    }
}
