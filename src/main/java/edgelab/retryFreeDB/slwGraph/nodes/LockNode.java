package edgelab.retryFreeDB.slwGraph.nodes;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Builder
public class LockNode extends TXNode{
    @Setter
    @Getter
    protected String tableName;
    protected Set<String> ids = new HashSet<>();



    @Override
    public String toString() {
        return "Lock " + tableName;
    }

    public void addId(String id) {
        ids.add(id);
    }
}
