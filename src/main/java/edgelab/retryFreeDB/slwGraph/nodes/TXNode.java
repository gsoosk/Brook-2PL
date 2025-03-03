package edgelab.retryFreeDB.slwGraph.nodes;
import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class TXNode {
    @Getter
    Set<TXNode> dependencies = new HashSet<>();
    public void addDependency(TXNode node) {
        dependencies.add(node);
    }

}
