package edgelab.retryFreeDB.slwGraph.nodes;

import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

public class SCNode extends TXNode{
    @Setter
    @Getter
    private String content;

    @Setter
    @Getter
    private List<LockNode> locks;

    @Override
    public String toString() {
        return content;
    }
}
