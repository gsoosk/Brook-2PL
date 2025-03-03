package edgelab.retryFreeDB.slwGraph;

import com.mxgraph.layout.mxCircleLayout;
import com.mxgraph.layout.mxIGraphLayout;
import com.mxgraph.util.mxCellRenderer;
import edgelab.retryFreeDB.slwGraph.nodes.LockNode;
import edgelab.retryFreeDB.slwGraph.nodes.ReadNode;
import edgelab.retryFreeDB.slwGraph.nodes.TXNode;
import edgelab.retryFreeDB.slwGraph.nodes.UnlockNode;
import edgelab.retryFreeDB.slwGraph.nodes.UpdateNode;
import edgelab.retryFreeDB.slwGraph.nodes.WriteNode;
import org.jgrapht.Graph;
import org.jgrapht.alg.cycle.HawickJamesSimpleCycles;
import org.jgrapht.ext.JGraphXAdapter;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleDirectedGraph;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SLWGraph {

    private final Graph<TXNode, DefaultEdge> graph = new SimpleDirectedGraph<>(DefaultEdge.class);
    /*
    * Gets a transaction in forms of READ -> Write -> ...
    * */


    private final List<List<TXNode>> chains = new ArrayList<>();
    public void addNewTransaction(TransactionRepresentation tx) {
        List<UpdateNode> updateNodes = new ArrayList<>();
        Map<String, TXNode> producedOutputs = new HashMap<>(); // output: udpatenode

        for (TransactionRepresentation.Operation op : tx.getOperations()) {
            UpdateNode node = switch (op.getType()) {
                case READ -> new ReadNode(op.getTableName(), op.getId());
                case WRITE -> new WriteNode(op.getTableName(), op.getId());
                default -> throw new IllegalArgumentException("Unsupported operation type: " + op.getType());
            };

            if (producedOutputs.containsKey(op.getId())) {
                node.addDependency(producedOutputs.get(op.getId()));
            }
            if (op.getOutputName() != null) {
                producedOutputs.put(op.getOutputName(), node);
            }
            updateNodes.add(node);
        }

        List<TXNode> transactionNodes = getTransactionChain(updateNodes);
        chains.add(transactionNodes);

        addChainToGraph(transactionNodes);
    }
    private void addChainToGraph(List<TXNode> chain) {
        for (TXNode txNode : chain)
            graph.addVertex(txNode);

        for (int i = 1; i < chain.size(); i++)
            graph.addEdge(chain.get(i-1), chain.get(i));

        addConflictEdges(chain);
    }

    private void addConflictEdges(List<TXNode> transactionNodes) {
        for(TXNode txNode: transactionNodes) {
            if (txNode instanceof LockNode) {
                for (TXNode toCheck: graph.vertexSet()) {
                    if (toCheck == txNode)
                        continue;

                    if (toCheck instanceof LockNode &&
                            ((LockNode) txNode).getTableName().equals(((LockNode) toCheck).getTableName())) {
                        graph.addEdge(toCheck, txNode);
                        graph.addEdge(txNode, toCheck);
                    }
                }

            }
        }
    }

    private List<TXNode> getTransactionChain(List<UpdateNode> transaction) {
        Map<String, LockNode> alreadyLocked = new HashMap<>();

        List<TXNode> transactionWithLocks = new ArrayList<>();
        for (UpdateNode node : transaction) {
            LockNode lock;
            //TODO: add shared lock
            if (!alreadyLocked.containsKey(node.getTableName())) {
                lock = LockNode.builder().tableName(node.getTableName()).ids(new HashSet<>()).build();
                lock.addId(node.getId());
                transactionWithLocks.add(lock);
                alreadyLocked.put(node.getTableName(), lock);
            } else {
                lock = alreadyLocked.get(node.getTableName());
                lock.addId(node.getId());
            }

            Set<TXNode> dependencies = node.getDependencies();
            for (TXNode dependency: dependencies) {
                lock.addDependency(dependency);
            }
            dependencies.clear();

            transactionWithLocks.add(node);
        }
        transactionWithLocks.add(UnlockNode.builder().toUnlockTables(alreadyLocked.keySet().stream().toList()).build());
        return transactionWithLocks;
    }


    public Boolean isThereADeadlock() {
        HawickJamesSimpleCycles <TXNode, DefaultEdge> cycleDetector = new HawickJamesSimpleCycles<>(graph);
        System.out.println(cycleDetector.findSimpleCycles());
        List<List<TXNode> > allCycles = cycleDetector.findSimpleCycles();
        for (List<TXNode> cycle :
                allCycles) {
            if (!areAllNodesSameLocks(cycle))
                return true;
        }
        return false;
    }

    private boolean areAllNodesSameLocks(List<TXNode> cycle) {
        for (int i = 1; i < cycle.size(); i++) {
            if (!(cycle.get(i-1) instanceof LockNode))
                return false;
            if (!(cycle.get(i) instanceof LockNode))
                return false;

            if (!((LockNode) cycle.get(i - 1)).getTableName().equals(((LockNode) cycle.get(i)).getTableName()))
                return false;
        }
        return true;
    }


    public void show() {
        File imgFile = new File("./graph.png");
        try {
            imgFile.createNewFile();
            JGraphXAdapter<TXNode, DefaultEdge> graphAdapter =
                    new JGraphXAdapter<TXNode, DefaultEdge>(graph);
            mxIGraphLayout layout = new mxCircleLayout(graphAdapter);
            layout.execute(graphAdapter.getDefaultParent());

            BufferedImage image =
                    mxCellRenderer.createBufferedImage(graphAdapter, null, 2, Color.WHITE, true, null);

            ImageIO.write(image, "PNG", imgFile);

        } catch (IOException e) {
            return;
        }
    }
}

