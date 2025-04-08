package edgelab.retryFreeDB.slwGraph.nodes;

import lombok.Builder;

public class WriteNode extends UpdateNode{
    public WriteNode(String tableName, String id) {
        super(tableName, id);
    }


    @Override
    public String toString() {
        return "Write " + tableName + ":<" + id + ">";
    }
}
