package edgelab.retryFreeDB.slwGraph.nodes;

import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

@Getter
public abstract class UpdateNode extends TXNode{

    public UpdateNode(String tableName, String id) {
        this.tableName = tableName;
        this.id = id;
    }

    @Setter
    protected String tableName;
    protected String id;
}
