package edgelab.retryFreeDB.slwGraph.nodes;

import lombok.Builder;

public class ReadNode extends UpdateNode{
    public ReadNode(String tableName, String id) {
        super(tableName, id);
    }

    @Override
    public String toString() {
        return "Read " + tableName + ":<" + id + ">";
    }
}
