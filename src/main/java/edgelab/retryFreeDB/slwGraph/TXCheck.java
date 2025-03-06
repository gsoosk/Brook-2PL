package edgelab.retryFreeDB.slwGraph;

import edgelab.retryFreeDB.init.InitStoreBenchmark;
import edgelab.retryFreeDB.init.InitTPCCBenchmark;
import edgelab.retryFreeDB.slwGraph.nodes.TXNode;
import lombok.extern.slf4j.Slf4j;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

@Slf4j
public class TXCheck {
    private static final SLWGraph slwGraph = new SLWGraph();
    private static String addListing = "TX(PId, IId, price):\n" +
            "X = Read(Item, IId)\n" +
            "Y = Read(Player, PId)\n" +
            "Insert(Listing)";

    private static String buyListing = "buyListing(PId, LId):\n" +
            "listing = Read(Listing, LId)\n" +
            "buyer = Read(Player, PId)\n" +
            "item = Read(Item, listing.item)\n" +
            "owner = Read(Player, item.owner)\n" +
            "Delete(Listing, LId)\n" +
            "Write(Item, listing.item)\n" +
            "Write(Player, PId)\n" +
            "Write(Player, item.owner)\n";

    private static String payment = "TX(warehouseId, districtId, paymentAmount, customerWarehouseId, customerDistrictId, customerId, txId):\n" +
            "W = Read(warehouse, warehouseId)\n" +
            "Write(warehouse, warehouseId)\n" +
            "D = Read(district, warehouseId_districtId)\n" +
            "Write(district, warehouseId_districtId)\n" +
            "C = Read(customer, customerWarehouseId_customerDistrictId_customerId)\n" +
            "Write(customer, customerWarehouseId_customerDistrictId_customerId)\n" +
            "Insert(history)";
    private static String newOrder = "TX(warehouseId, districtId, customerId, orderLineCount, allLocals, itemIDs, supplierWarehouseIDs, orderQuantities, txId):\n" +
            "C = Read(customer, warehouseId_districtId_customerId)\n" +
            "W = Read(warehouse, warehouseId)\n" +
            "D = Read(district, warehouseId_districtId)\n" +
            "Write(district, warehouseId_districtId)\n" +
            "Insert(oorder)\n" +
            "Insert(new_order)\n" +
            "I = Read(item, itemIDs)\n" +
            "S = Read(stock, supplierWarehouseIDs_itemIDs)\n" +
            "Insert(order_line)\n" +
            "Write(stock, supplierWarehouseIDs_itemIDs)";


    private static Map<String, Integer> storePopulation = Map.of(
            "Item", InitStoreBenchmark.NUM_OF_EACH_PLAYER_ITEMS * InitStoreBenchmark.NUM_OF_PLAYERS,
            "Player", InitStoreBenchmark.NUM_OF_PLAYERS,
            "Listing", InitStoreBenchmark.NUM_OF_INITIAL_LISTINGS
    );


    private static Map<String, Integer> tpccPopulation = Map.of(
            "warehouse", 1,
            "district", InitTPCCBenchmark.CONFIG_DIST_PER_WAREHOUSE,
            "customer", InitTPCCBenchmark.CONFIG_CUST_PER_DISTRICT * InitTPCCBenchmark.CONFIG_DIST_PER_WAREHOUSE,
            "item", InitTPCCBenchmark.CONFIG_ITEM_COUNT,
            "stock", InitTPCCBenchmark.CONFIG_ITEM_COUNT
    );

    public static void main(String args[])
    {

//        slwGraph.setPopulation(storePopulation);
//        slwGraph.addNewTransaction(TransactionParser.parseTransaction(addListing));
//        slwGraph.addNewTransaction(TransactionParser.parseTransaction(buyListing));
//
        slwGraph.setPopulation(tpccPopulation);
        slwGraph.addNewTransaction(TransactionParser.parseTransaction(payment));
        slwGraph.addNewTransaction(TransactionParser.parseTransaction(newOrder));


//        graph.show();
        System.out.println(slwGraph.isThereADeadlock() ? "There is a deadlock" : "There is no deadlock");
        System.out.println("-------Original Chains-----------");
        slwGraph.printChains();
        System.out.println("------------------");

        List<List<List<TXNode>>> deadlockFreeTransactions =  slwGraph.generateAllDeadlockFreeTransactions();

        // Sort the transaction sets based on their score in ascending order.
        deadlockFreeTransactions.sort(Comparator.comparingDouble(slwGraph::getChainsScore));

        if (!deadlockFreeTransactions.isEmpty()) {

            System.out.println("There is " + deadlockFreeTransactions.size() + " possible reordered ways to have deadlock-free transactions");

            for (int i = 0; i < Math.min(10, deadlockFreeTransactions.size()); i++) {
                System.out.println("------Best Transactions After Reordering (" + i +")-----------");
                System.out.println("Score: " + slwGraph.getChainsScore(deadlockFreeTransactions.get(i)));
                SLWGraph.printChains(deadlockFreeTransactions.get(i));
                System.out.println("------------------");
            }

        }
        else {
            System.out.println("There is no possible reordered way to have deadlock-free transactions");
        }

    }
}
