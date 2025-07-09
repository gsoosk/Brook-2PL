package edgelab.retryFreeDB.slwGraph;

import com.mxgraph.layout.mxCircleLayout;
import com.mxgraph.layout.mxIGraphLayout;
import com.mxgraph.util.mxCellRenderer;
import edgelab.retryFreeDB.slwGraph.nodes.LockNode;
import edgelab.retryFreeDB.slwGraph.nodes.ReadNode;
import edgelab.retryFreeDB.slwGraph.nodes.SCNode;
import edgelab.retryFreeDB.slwGraph.nodes.TXNode;
import edgelab.retryFreeDB.slwGraph.nodes.UnlockNode;
import edgelab.retryFreeDB.slwGraph.nodes.UpdateNode;
import edgelab.retryFreeDB.slwGraph.nodes.WriteNode;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.checkerframework.checker.units.qual.A;
import org.jgrapht.Graph;
import org.jgrapht.alg.cycle.CycleDetector;
import org.jgrapht.alg.cycle.HawickJamesSimpleCycles;
import org.jgrapht.alg.cycle.JohnsonSimpleCycles;
import org.jgrapht.ext.JGraphXAdapter;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleDirectedGraph;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.jgrapht.graph.SimpleGraph;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

@Slf4j
public class SLWGraph {


    /*
    * Gets a transaction in forms of READ -> Write -> ...
    * */


    private final List<List<TXNode>> chains = new ArrayList<>();
    @Setter
    private Map<String, Integer> population = new HashMap<>();

    public static <T> Predicate<List<List<TXNode>>> distinctByRepresentation() {
        Set<Object> seen = ConcurrentHashMap.newKeySet();
        return t -> seen.add(chainsString(t));
    }

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
                double tableContentionScore = getTableContentionScore(((LockNode) chain.get(i)).getTableName());
                double lockTypeImportance = ((LockNode) chain.get(i)).getType() == LockNode.Type.EXCLUSIVE ? 1 : 1;
                double lockContentionPortion  = lockOperationNumbers(chain, i);

                score += lockContentionPortion * lockTypeImportance * tableContentionScore;

            }
        }
        return score;
    }

    private double getTableContentionScore(String tableName) {
        return this.population.containsKey(tableName) ? (double) 1 / this.population.get(tableName) : 1;
    }

    private double lockOperationNumbers(List<TXNode> chain, int i) {
        LockNode lockNode = (LockNode) chain.get(i);
        double afterLock = 0;
        for (int j = i+1; j < chain.size(); j++) {
            if (chain.get(j) instanceof UpdateNode) {
                afterLock++;
            }
            if (chain.get(j) instanceof UnlockNode && ((UnlockNode) chain.get(j)).getToUnlockTables().contains(lockNode.getTableName()))
                break;
        }

        return afterLock;
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
            node.setCommutative(op.isCommutative());
            node.setSelfCommutative(op.isSelfCommutative());
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
            if (!node.isCommutative()) {
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
                for (TXNode dependency : dependencies) {
                    lock.addDependency(dependency);
                }
                dependencies.clear();

                if (node instanceof ReadNode) // We can also move read nodes but not before their locks
                    dependencies.add(lock);

                lock.setSelfCommutative(node.isSelfCommutative());
            }



            transactionWithLocks.add(node);
        }
        List<String> toUnlockTables = alreadyLocked.keySet().stream().toList();
        transactionWithLocks.add(UnlockNode.builder().toUnlockTables(toUnlockTables).build());
        return transactionWithLocks;
    }


    public Boolean isThereADeadlock() {
        Graph<TXNode, DefaultEdge> graph = getGraphOfChains(this.chains);
        return doesGraphHasSLWCycle(graph);
    }

    public boolean isItSerializable() {
        Graph<TXNode, SCEdge> graph = getSCGraphOfChains(this.chains);


       return false;
    }

    static class SCEdge extends DefaultEdge {
        enum Type {
            S, C
        }

        private final Type type;

        public SCEdge(Type type) {
            this.type = type;
        }

        public Type getType() {
            return type;
        }
    }

    private Graph<TXNode, SCEdge> getSCGraphOfChains(List<List<TXNode>> chains) {
        List<List<TXNode>> scChains = new ArrayList<>();
        for (List<TXNode> chain: chains) {
            List<TXNode> scChain = new ArrayList<>();
            List<TXNode> duplicateChain = new ArrayList<>();
            StringBuilder currentContent = new StringBuilder();
            List<LockNode> locks = new ArrayList<>();
            for (int i = 0; i < chain.size(); i++) {
                currentContent.append(chain.get(i).toString() + "->");
                if (chain.get(i) instanceof LockNode)
                    locks.add((LockNode) chain.get(i));
                else if (chain.get(i) instanceof UnlockNode) {
                    if (i != chain.size() - 1 && chain.get(i+1) instanceof UnlockNode)
                        continue;

                    SCNode scNode = new SCNode();
                    scNode.setContent(currentContent.toString());
                    scNode.setLocks(locks);
                    scChain.add(scNode);

                    // Add a duplicate node to the duplicateChain
                    SCNode duplicateNode = new SCNode();
                    duplicateNode.setContent(currentContent.toString());
                    duplicateNode.setLocks(new ArrayList<>(locks));
                    duplicateChain.add(duplicateNode);

                    currentContent.setLength(0); // clear
                    locks = new ArrayList<>();
                }
            }
            scChains.add(scChain);
            scChains.add(duplicateChain);
            // You can also add duplicateChain to another list if needed
        }


        Graph<TXNode, SCEdge> graph = new SimpleDirectedGraph<>(SCEdge.class);
        for (List<TXNode> chain: scChains) {
            for (TXNode node : chain) {
                graph.addVertex(node);
            }

            for (int i = 0; i < chain.size() - 1; i++) {
                graph.addEdge(chain.get(i), chain.get(i+1), new SCEdge(SCEdge.Type.S));
                graph.addEdge(chain.get(i+1), chain.get(i), new SCEdge(SCEdge.Type.S));
            }
        }


        for (List<TXNode> chain : scChains) {
            for (TXNode node : chain) {
                for (TXNode otherNode : graph.vertexSet()) {
                        if (otherNode == node) continue;
                        // Check for conflicting locks between node and otherNode
                        if (node instanceof SCNode scNode1 && otherNode instanceof SCNode scNode2) {
                            for (LockNode lock1 : scNode1.getLocks()) {
                                for (LockNode lock2 : scNode2.getLocks()) {
                                    if (lock1.getTableName().equals(lock2.getTableName()) &&
                                        (lock1.getType() == LockNode.Type.EXCLUSIVE || lock2.getType() == LockNode.Type.EXCLUSIVE)) {
                                        graph.addEdge(node, otherNode, new SCEdge(SCEdge.Type.C));
                                        graph.addEdge(otherNode, node, new SCEdge(SCEdge.Type.C));
                                    }
                                }
                        }
                    }
                }
            }
        }



        return graph;
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


    private Set<String> conflictingTables = new HashSet<>();
    public List<List<List<TXNode>>> generateAllDeadlockFreeTransactions() {

        updateAllConflictingTables();
        log.info("Getting all chain permutations...");
        List<Set<List<TXNode>>> chainPermutations = this.chains.parallelStream().map(this::getAllChainPermutations).toList();

        log.info("Get all the chains possible from chain permutations...");
        List<List<List<TXNode>>> deadLockFreeTransactions = cartesianProductParallel(chainPermutations);
        log.info("Save deadlock free permutations...");
        deadLockFreeTransactions = deadLockFreeTransactions.parallelStream().filter(chain -> {
            Graph<TXNode, DefaultEdge> graph = getGraphOfChains(chain);
            return !doesGraphHasSLWCycle(graph);
        }).distinct().collect(Collectors.toList());
//        cartesianProductGraphs(chainPermutations, 0, new ArrayList<>(), deadLockFreeTransactions);

        return deadLockFreeTransactions;
    }

    private void updateAllConflictingTables() {
        for (List<TXNode> chain : chains){
            for (TXNode node : chain) {
                if (node instanceof LockNode l && hasConflictWithOtherChains(chain, chains, l)) {
                    conflictingTables.add(l.getTableName());
                }
            }
        }
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
            if (node instanceof LockNode l && conflictingTables.contains(l.getTableName()))
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
                backtrack(moveNodeEarlierInChain(current, nodePos, move), i+1, movableNodes, assignments);
            }
        }
    }


    private List<TXNode> moveNodeEarlierInChain(List<TXNode> current, int nodePos, int move) {
        //        0, 1, 2, 3, 4, 5
//        pos = 3, move 1 -> newPos = 2
        int newPos = nodePos - move;
        List<TXNode> copy = new ArrayList<>(current);
        moveNodeInChain(copy, nodePos, newPos);
        return copy;
    }

    private void moveNodeInChain(List<TXNode> chain, int nodePos, int newPos) {
        chain.add(newPos, chain.get(nodePos));
        if (newPos >= nodePos)
            chain.remove(nodePos);
        else
            chain.remove(nodePos + 1);
    }

    private int getNodePosition(List<TXNode> current, TXNode node) {
        for (int i = 0; i < current.size(); i++) {
            if (node == current.get(i)) {
                return i;
            }
        }

//        throw new Exception("Could not find the node in current to calcualte possible moves");
        return -1;
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

    public static String chainsString(List<List<TXNode>> chains) {
        StringBuilder chainStr = new StringBuilder();
        for (List<TXNode> chain:
             chains) {

            for (int i = 0; i < chain.size(); i++) {
                chainStr.append(chain.get(i));
                if (i != chain.size() - 1)
                    chainStr.append(" -> ");
            }
            chainStr.append("\n");
        }
        return chainStr.toString();
    }

    public static void printChains(List<List<TXNode>> chains) {
        System.out.print(chainsString(chains));
    }

    public void printChains() {
        printChains(chains);
    }

    public List<List<TXNode>> releaseLocksEarlier(List<List<TXNode>> chains, List<String> toReleaseTables) {



//        lx -> wx -> ux

//        ux: dependency: last write/read x
        List<List<TXNode>> chainsWithNewUnlock = extractUnlockNode(chains, toReleaseTables);
        moveUnlockNodesAsEarlyAsPossible(chainsWithNewUnlock, toReleaseTables);
        return chainsWithNewUnlock;
//        validateSCGraph(chains);


    }

    private void moveUnlockNodesAsEarlyAsPossible(List<List<TXNode>> chains, List<String> toReleaseTables) {
//        moves unlock one by one earlier
//        if a lock is on the way, the lock also moves with unlock earlier
//        (In the final SC-Graph only first node will create conflicts (C-edges))

        for (List<TXNode> chain : chains) {
            for (String table : toReleaseTables) {

//                find unlock node
                int initialNodePos = -1;
                for (int node = chain.size() - 1; node >= 0; node--) {
                    if (chain.get(node) instanceof UnlockNode && ((UnlockNode) chain.get(node)).getToUnlockTables().contains(table)) {
                        initialNodePos = node;
                        break;
                    }
                }
                if (initialNodePos == -1)
                    continue;

//              move unlock node (and conflicting locking nodes on the way) until its last operation
                int afterPos = initialNodePos + 1;
                boolean stopMoving = false;
                for (int i = initialNodePos - 1; i >= 0; i--) {
//                    also move (ex locking nodes that create self-conflicts /  locking nodes that conflicts with other chains)
                    if (chain.get(i) instanceof LockNode lockNode)
                    {
                        if (!lockNode.isSelfCommutative() && lockNode.getType() == LockNode.Type.EXCLUSIVE)
                            continue;

                        if (hasConflictWithOtherChains(chain, chains, lockNode))
                            continue;
                    }






                    double improvement = 0;
                    for (int j = i + 1; j < afterPos; j++) {
                        // should stop if we reach dependecies
                        if (chain.get(j).getDependencies().contains(chain.get(i))) {
                            stopMoving = true;
                            break;
                        }
//                        calculate improvement for the next lock to see if it worth moving
                        if (chain.get(j) instanceof UnlockNode u && u.getToUnlockTables().size() == 1)
                            improvement += getTableContentionScore(u.getToUnlockTables().get(0));
                        else if(chain.get(j) instanceof LockNode l)
                            improvement -= getTableContentionScore(l.getTableName());
                    }

                    if (stopMoving || improvement < 0)
                        break;

                    moveNodeInChain(chain, i, afterPos);
                    afterPos -= 1;

                }


            }
        }

    }

    private boolean hasConflictWithOtherChains(List<TXNode> chain, List<List<TXNode>> chains, LockNode node) {
        for (List<TXNode> chainsToCheck :
                chains) {
            if (chainsToCheck.equals(chain))
                continue;

            for (TXNode toCheckNode: chainsToCheck)
                if (toCheckNode instanceof LockNode
                        && (((LockNode) toCheckNode).getType() == LockNode.Type.EXCLUSIVE || node.getType() == LockNode.Type.EXCLUSIVE)
                        && ((LockNode) toCheckNode).getTableName().equals(node.getTableName()))
                    return true;

        }
        return false;
    }


    private List<List<TXNode>> extractUnlockNode(List<List<TXNode>> chains, List<String> toReleaseTables) {
        List<List<TXNode>> newChains = new ArrayList<>(chains);
        for (int i = 0 ; i < newChains.size() ; i++) {
            List<TXNode> chain = new ArrayList<>(newChains.get(i));
            newChains.set(i, chain);

            if (chain.isEmpty())
                continue;

            TXNode unlockNode = chain.get(chain.size() - 1);
            if (!(unlockNode instanceof UnlockNode))
                continue;

            List<String> currentTables = new ArrayList<>(((UnlockNode) unlockNode).getToUnlockTables());
            for (String toReleaseTable : toReleaseTables) {
                if (currentTables.contains(toReleaseTable)) {
                    UnlockNode newUnlockNode = UnlockNode.builder().movable(true).toUnlockTables(List.of(toReleaseTable)).build();
                    chain.add(newUnlockNode);
                    newUnlockNode.addDependency(getPreviousUpdate(chain, newUnlockNode));
                    currentTables.remove(toReleaseTable);
                }
            }
            if (currentTables.isEmpty()) {
                chain.remove(unlockNode);
            }
            else {
                chain.remove(unlockNode);
                chain.add(UnlockNode.builder().movable(false).toUnlockTables(new ArrayList<>(currentTables)).build());
//                ((UnlockNode) unlockNode).setToUnlockTables(currentTables);
            }
        }
        return newChains;
    }

    private TXNode getPreviousUpdate(List<TXNode> chain, UnlockNode newUnlockNode) {
        int unlockIndex = chain.indexOf(newUnlockNode);
        for (int i = unlockIndex - 1; i >=0 ; i--) {
            if (chain.get(i) instanceof UpdateNode && newUnlockNode.getToUnlockTables().contains(((UpdateNode) chain.get(i)).getTableName()))
                return chain.get(i);
        }
        return null;
    }


    public List<List<List<TXNode>>> getAllPartiallyChoppedTransactions(List<List<List<TXNode>>> transactions, List<String> toReleaseLocksTable) {
        log.info("Partially chopping deadlock-free permutations...");
        return transactions.stream()
                .map(chains -> releaseLocksEarlier(new ArrayList<>(chains), toReleaseLocksTable)).collect(Collectors.toList());
    }



    public void loadChainsFromJson(String jsonTransactions) throws JSONException {
        JSONArray txArray = new JSONArray(jsonTransactions);
        for (int ti = 0; ti < txArray.length(); ti++) {
            JSONObject txObj = txArray.getJSONObject(ti);
            JSONArray stmts = txObj.getJSONArray("statements");

            List<TXNode> chain = new ArrayList<>();
            for (int si = 0; si < stmts.length(); si++) {
                JSONObject s = stmts.getJSONObject(si);
                String type = s.getString("type").toLowerCase();
                String tbl = s.getString("table");
                String key = s.optString("key", null);
                boolean comm = s.optBoolean("commutative", false);
                // helper to split comma‐list into set of IDs
                Set<String> ids = key == null
                        ? Set.of()
                        : Arrays.stream(key.split("\\s*,\\s*")).collect(Collectors.toSet());

                switch (type) {
                    case "shared_lock": {
                        LockNode lock = LockNode.builder()
                                .tableName(tbl)
                                .ids(new HashSet<>(ids))
                                .type(LockNode.Type.SHARED)
                                .build();
                        chain.add(lock);
                        break;
                    }
                    case "exclusive_lock": {
                        LockNode lock = LockNode.builder()
                                .tableName(tbl)
                                .ids(new HashSet<>(ids))
                                .type(LockNode.Type.EXCLUSIVE)
                                .build();
                        chain.add(lock);
                        break;
                    }
                    case "read": {
                        ReadNode r = new ReadNode(tbl, key);
                        r.setCommutative(comm);
                        // if an "output" field is present, treat as a dependence placeholder:
                        if (s.has("output")) {
                            r.setOutputName(s.getString("output"));
                        }
                        chain.add(r);
                        break;
                    }
                    case "write":
                    case "delete":
                    case "insert": {
                        WriteNode w = new WriteNode(tbl, key);
                        w.setCommutative(comm);
                        chain.add(w);
                        break;
                    }
                    case "unlock": {
                        UnlockNode u = UnlockNode.builder()
                                .toUnlockTables(List.of(tbl))
                                .build();
                        // find the most recent LockNode on this table and make u depend on it
                        for (int back = chain.size() - 1; back >= 0; back--) {
                            TXNode n = chain.get(back);
                            if (n instanceof LockNode ln && ln.getTableName().equals(tbl)) {
                                u.addDependency(n);
                                break;
                            }
                        }
                        chain.add(u);
                        break;
                    }
                    default:
                        throw new IllegalArgumentException("Unknown statement type: " + type);
                }
            }

            // add exactly in this order:
            this.chains.add(chain);
        }
    }

}

