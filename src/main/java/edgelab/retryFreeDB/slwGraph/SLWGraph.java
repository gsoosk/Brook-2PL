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
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class SLWGraph {


    /*
    * Gets a transaction in forms of READ -> Write -> ...
    * */


    private final List<List<TXNode>> chains = new ArrayList<>();
    @Setter
    private Map<String, Integer> population = new HashMap<>();

    public double getChainsScore(List<List<TXNode>> chains) {
        if (chains.size() != this.chains.size())
            throw new RuntimeException("chains should be equal in size");
        double score = 0;
        for (int i = 0 ; i < chains.size() ; i++) {
            score += getChainContentionScore(chains.get(i)) * 10;
//            score += getDiff(chains.get(i), this.chains.get(i));
        }
        return score;
    }

    private double getDiff(List<TXNode> c1, List<TXNode> c2) {
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

        return (double) diff / c1.size();
    }

    private double getChainContentionScore(List<TXNode> chain) {
        double score  = 0;
        for (int i = 0 ; i < chain.size() ; i++) {
            if (chain.get(i) instanceof LockNode ) {
                double tableContentionScore = this.population.containsKey(((LockNode) chain.get(i)).getTableName()) ? (double) 1 / this.population.get(((LockNode) chain.get(i)).getTableName()): 1;
                double lockTypeImportance = ((LockNode) chain.get(i)).getType() == LockNode.Type.EXCLUSIVE ? 1 : 1;
                double lockContentionPortion  = getLockContentionPortion(chain, i);

                score += lockContentionPortion * lockTypeImportance * tableContentionScore;

            }
        }
        return score;
    }

    private double getLockContentionPortion(List<TXNode> chain, int i) {
        int chainSize = 0;
        int afterLock = 0;
        for (int j = 0; j < chain.size(); j++) {
            if (!(chain.get(j) instanceof LockNode)) {
                chainSize++;
                if (j > i)
                    afterLock++;
            }



        }
        return (double) afterLock;
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

    private static Graph<TXNode, DefaultEdge> getGraphOfChains(List<List<TXNode>> chains) {
        Graph<TXNode, DefaultEdge> graph = new SimpleDirectedGraph<>(DefaultEdge.class);
        for (List<TXNode> chain :
                chains) {
            addChainToGraph(graph, chain);
        }
        return graph;
    }
    private static void addChainToGraph(Graph<TXNode, DefaultEdge> graph, List<TXNode> chain) {
        for (TXNode txNode : chain)
            graph.addVertex(txNode);

        for (int i = 1; i < chain.size(); i++)
            graph.addEdge(chain.get(i-1), chain.get(i));

        addConflictEdges(graph, chain);
    }

    private static void addConflictEdges(Graph<TXNode, DefaultEdge> graph, List<TXNode> transactionNodes) {
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

    private static Boolean doesGraphHasSLWCycle(Graph<TXNode, DefaultEdge> graph) {
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
        log.info("Getting all chain permutations...");
        List<Set<List<TXNode>>> chainPermutations = this.chains.parallelStream().map(this::getAllChainPermutations).toList();

        log.info("Get all the chains possible from chain permutations...");
        List<List<List<TXNode>>> deadLockFreeTransactions = cartesianProductParallel(chainPermutations);
        log.info("Save deadlock free permutations...");
        deadLockFreeTransactions = deadLockFreeTransactions.parallelStream().filter(chain -> {
            Graph<TXNode, DefaultEdge> graph = getGraphOfChains(chain);
            return !doesGraphHasSLWCycle(graph);
        }).collect(Collectors.toList());
//        cartesianProductGraphs(chainPermutations, 0, new ArrayList<>(), deadLockFreeTransactions);

        return deadLockFreeTransactions;
    }
    public static <T> List<List<T>> cartesianProductParallel(List<Set<T>> lists) {
        List<List<T>> result = new ArrayList<>();
        // Start with an empty combination.
        result.add(new ArrayList<>());

        // For each list (or "pool") in the input, expand combinations in parallel.
        for (Set<T> pool : lists) {
            result = result.parallelStream()
                    .flatMap(partial -> pool.parallelStream()
                            .map(item -> {
                                List<T> newPartial = new ArrayList<>(partial);
                                newPartial.add(item);
                                return newPartial;
                            }))
                    .collect(Collectors.toList());
        }
        return result;
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
        List<TXNode> movableNodes = new ArrayList<>();
        for (TXNode node :
                chain) {
            if (node instanceof LockNode)
                movableNodes.add(node);
            if (node instanceof ReadNode)
                for (TXNode otherNode :
                        chain) {
                    if (otherNode.getDependencies().contains(node)) {
                        movableNodes.add(node);
                        break;
                    }
                }
        }

        return movableNodes;
    }

    private void backtrack(List<TXNode> current, int currentNode, List<TXNode> movableNodes, Set<List<TXNode>> assignments) throws Exception {
        // if all the locking nodes are moved somewhere earlier
//        System.out.println(currentNode);
        if (currentNode == movableNodes.size()) {
            if (validateAssignment(current)) {
                assignments.add(new ArrayList<>(current));
            }
            return;
        }

        for (int i = currentNode; i < movableNodes.size(); i++) {
            TXNode node = movableNodes.get(i);
            int nodePos = getNodePosition(current, node);
//            List<Integer> moves =  IntStream.range(0, nodePos+1).boxed().toList();
            List<Integer> moves = getNodeMoves(current, nodePos, movableNodes);

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


    private List<Integer> getNodeMoves(List<TXNode> current, int nodePos, List<TXNode> movableNodes) {
        List<Integer> moves = new ArrayList<>();
        for (int i = 0; i <= nodePos; i++) {
            if(movableNodes.contains(current.get(i)))
                moves.add(i);
        }
        return moves;
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

    private static boolean areAllNodesSameLocks(List<TXNode> cycle) {
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

