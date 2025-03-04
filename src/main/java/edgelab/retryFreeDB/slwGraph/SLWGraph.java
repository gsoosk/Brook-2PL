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
import java.util.stream.IntStream;

public class SLWGraph {


    /*
    * Gets a transaction in forms of READ -> Write -> ...
    * */


    private final List<List<TXNode>> chains = new ArrayList<>();

    public int getChainsScore(List<List<TXNode>> chains) {
        if (chains.size() != this.chains.size())
            throw new RuntimeException("chains should be equal in size");
        int score = 0;
        for (int i = 0 ; i < chains.size() ; i++) {
            score += getChainContentionScore(chains.get(i)) * 10;
            score += getDiff(chains.get(i), this.chains.get(i));
        }
        return score;
    }

    private int getDiff(List<TXNode> c1, List<TXNode> c2) {
        int diff = 0;
        int i = 0, j = 0;
        while (i < c1.size() && j < c2.size()) {
            if (c1.get(i) instanceof UpdateNode && c2.get(j) instanceof UpdateNode) {
                if (c1.get(i) != c2.get(j))
                    diff++;
                i++;
                j++;
            }

            if (!(c1.get(i) instanceof UpdateNode)) {
                i++;
            }
            if (!(c2.get(j) instanceof UpdateNode)) {
                j++;
            }
        }

        return diff;
    }

    private int getChainContentionScore(List<TXNode> chain) {
        int score  = 0;
        for (int i = 0 ; i < chain.size() ; i++) {
            if (chain.get(i) instanceof LockNode ) {
                int mult = ((LockNode) chain.get(i)).getType() == LockNode.Type.EXCLUSIVE ? 5 : 1;
                score += ((chain.size() - i) - 1) * mult;
            }
        }
        return score;
    }

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
    }

    private Graph<TXNode, DefaultEdge> getGraphOfChains(List<List<TXNode>> chains) {
        Graph<TXNode, DefaultEdge> graph = new SimpleDirectedGraph<>(DefaultEdge.class);
        for (List<TXNode> chain :
                chains) {
            addChainToGraph(graph, chain);
        }
        return graph;
    }
    private void addChainToGraph(Graph<TXNode, DefaultEdge> graph, List<TXNode> chain) {
        for (TXNode txNode : chain)
            graph.addVertex(txNode);

        for (int i = 1; i < chain.size(); i++)
            graph.addEdge(chain.get(i-1), chain.get(i));

        addConflictEdges(graph, chain);
    }

    private void addConflictEdges(Graph<TXNode, DefaultEdge> graph, List<TXNode> transactionNodes) {
        for(TXNode txNode: transactionNodes) {
            if (txNode instanceof LockNode) {
                for (TXNode toCheck: graph.vertexSet()) {
                    if (toCheck == txNode)
                        continue;

                    if (toCheck instanceof LockNode &&
                            ((LockNode) txNode).getTableName().equals(((LockNode) toCheck).getTableName()) &&
                            (((LockNode) txNode).getType() == LockNode.Type.EXCLUSIVE || ((LockNode) toCheck).getType() == LockNode.Type.EXCLUSIVE)) {
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
        for (int i = 0 ; i < transaction.size(); i++) {
            UpdateNode node = transaction.get(i);
            LockNode lock;
            if (!alreadyLocked.containsKey(node.getTableName())) {
                lock = LockNode.builder().tableName(node.getTableName()).ids(new HashSet<>()).build();
                lock.addId(node.getId());
                transactionWithLocks.add(lock);
                alreadyLocked.put(node.getTableName(), lock);
            } else {
                lock = alreadyLocked.get(node.getTableName());
                lock.addId(node.getId());
            }

            if (lock.getType() == null)
                lock.setType(LockNode.Type.SHARED);
            if (node instanceof WriteNode)
                lock.setType(LockNode.Type.EXCLUSIVE);

            Set<TXNode> dependencies = node.getDependencies();
            for (TXNode dependency: dependencies) {
                lock.addDependency(dependency);
            }
            dependencies.clear();

            if (node instanceof ReadNode) // We can also move read nodes but not before their locks
                dependencies.add(lock);



            transactionWithLocks.add(node);
        }
        transactionWithLocks.add(UnlockNode.builder().toUnlockTables(alreadyLocked.keySet().stream().toList()).build());
        return transactionWithLocks;
    }


    public Boolean isThereADeadlock() {
        Graph<TXNode, DefaultEdge> graph = getGraphOfChains(this.chains);
        return doesGraphHasSLWCycle(graph);
    }

    private Boolean doesGraphHasSLWCycle(Graph<TXNode, DefaultEdge> graph) {
        HawickJamesSimpleCycles <TXNode, DefaultEdge> cycleDetector = new HawickJamesSimpleCycles<>(graph);
        List<List<TXNode> > allCycles = cycleDetector.findSimpleCycles();
        for (List<TXNode> cycle :
                allCycles) {
            if (!areAllNodesSameLocks(cycle))
                return true;
        }
        return false;
    }


    public List<List<List<TXNode>>> generateAllDeadlockFreeTransactions() {
        List<List<List<TXNode>>> deadLockFreeTransactions = new ArrayList<>();

        List<Set<List<TXNode>>> chainPermutations = new ArrayList<>();
        for (List<TXNode> chain :
                chains) {
            chainPermutations.add(getAllChainPermutations(chain));
        }


        List<List<List<TXNode>>> allProducts = new ArrayList<>();
        cartesianProduct(chainPermutations, 0, new ArrayList<>(), allProducts);

        for (List<List<TXNode>> product : allProducts) {
            Graph<TXNode, DefaultEdge> graph = getGraphOfChains(product);
            if (!doesGraphHasSLWCycle(graph))
                deadLockFreeTransactions.add(product);
        }

        return deadLockFreeTransactions;
    }
    private void cartesianProduct(List<Set<List<TXNode>>> lists, int index,
                                  List<List<TXNode>> current,
                                  List<List<List<TXNode>>> result) {
        if (index == lists.size()) {
            result.add(new ArrayList<>(current));
            return;
        }
        for (List<TXNode> ordering : lists.get(index)) {
            current.add(ordering);
            cartesianProduct(lists, index + 1, current, result);
            current.remove(current.size() - 1);
        }
    }

    public Set<List<TXNode>> getAllChainPermutations(List<TXNode> chain) {
        Set<List<TXNode>> assignments = new HashSet<>();
        try {
            backtrack(new ArrayList<>(chain), 0, getMovableNodes(chain), assignments);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return assignments;
    }

    private List<TXNode> getMovableNodes(List<TXNode> chain) {
        List<TXNode> lockingNodes = new ArrayList<>();
        for (TXNode node :
                chain) {
            if (node instanceof LockNode || node instanceof ReadNode)
                lockingNodes.add(node);
        }

        return lockingNodes;
    }

    private void backtrack(List<TXNode> current, int currentNode, List<TXNode> movableNodes, Set<List<TXNode>> assignments) throws Exception {
        // if all the locking nodes are moved somewhere earlier
        if (currentNode == movableNodes.size()) {
            if (validateAssignment(current)) {
                assignments.add(new ArrayList<>(current));
            }
            return;
        }

        for (int i = currentNode; i < movableNodes.size(); i++) {
            TXNode node = movableNodes.get(i);
            int nodePos = getNodePosition(current, node);
            List<Integer> moves =  IntStream.range(0, nodePos+1).boxed().toList();

            for (int move : moves) {
                backtrack(moveNodeInChain(current, nodePos, move), i+1, movableNodes, assignments);
            }
        }
    }

    private List<TXNode> moveNodeInChain(List<TXNode> current, int nodePos, int move) {
        List<TXNode> copy = new ArrayList<>(current);
//        0, 1, 2, 3, 4, 5
//        pos = 3, move 1 -> newPos = 2
        int newPos = nodePos - move;
        TXNode node = copy.remove(nodePos);
        copy.add(newPos, node);
        return copy;
    }

    private int getNodePosition(List<TXNode> current, TXNode node) throws Exception {
        for (int i = 0; i < current.size(); i++) {
            if (node == current.get(i)) {
                return i;
            }
        }

        throw new Exception("Could not find the node in current to calcualte possible moves");
    }

    private boolean validateAssignment(List<TXNode> current) {
        for (int i = 0; i < current.size(); i++) {
            if (current.get(i) instanceof LockNode || current.get(i) instanceof ReadNode) {

                for (TXNode dep : current.get(i).getDependencies()) {
                    for (int j = 0 ; j < current.size() ; j++) {
                        if (current.get(j) == dep) {
                            if (j > i)
                                return false;
                            break;
                        }
                    }
                }

            }
        }
        return true;
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
        Graph<TXNode, DefaultEdge> graph = getGraphOfChains(chains);
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

    public static void printChains(List<List<TXNode>> chains) {
        for (List<TXNode> chain:
             chains) {
            StringBuilder chainStr = new StringBuilder();
            for (int i = 0; i < chain.size(); i++) {
                chainStr.append(chain.get(i));
                if (i != chain.size() - 1)
                    chainStr.append(" -> ");
            }
            System.out.println(chainStr);
        }
    }

    public void printChains() {
        printChains(chains);
    }
}

