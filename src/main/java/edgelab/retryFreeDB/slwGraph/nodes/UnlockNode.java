package edgelab.retryFreeDB.slwGraph.nodes;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.Objects;

@Builder
public class UnlockNode extends TXNode {
    @Setter
    @Getter
    protected List<String> toUnlockTables;

    @Getter
    @Setter
    private boolean movable = false;



    @Override
    public String toString() {
        return "Unlock " + toUnlockTables.toString() ;
    }


//    @Override
//    public int hashCode() {
//        return  Objects.hash(toUnlockTables, movable, dependencies);
//    }
}
