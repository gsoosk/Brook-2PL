package edgelab.retryFreeDB.slwGraph.nodes;

import lombok.Builder;
import lombok.Setter;

public class ReadNode extends UpdateNode{
    public ReadNode(String tableName, String id) {
        super(tableName, id);
    }

    @Setter
    private String outputName;

    @Override
    public String toString() {
        return "Read " + tableName + ":<" + id + ">";
    }
}
