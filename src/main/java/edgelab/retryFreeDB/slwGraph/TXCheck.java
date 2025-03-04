package edgelab.retryFreeDB.slwGraph;

import edgelab.retryFreeDB.slwGraph.nodes.TXNode;

import java.util.List;

public class TXCheck {
    private static final SLWGraph slwGraph = new SLWGraph();
    public static void main(String args[])
    {

        String addListing = "TX(PId, IId, price):\n" +
                "X = Read(Item, IId)\n" +
                "Y = Read(Player, PId)\n" +
                "Insert(Listing)";

        String buyListing = "buyListing(PId, LId):\n" +
                "listing = Read(Listing, LId)\n" +
                "buyer = Read(Player, PId)\n" +
                "item = Read(Item, listing.item)\n" +
                "owner = Read(Player, item.owner)\n" +
                "Delete(Listing, LId)\n" +
                "Write(Item, listing.item)\n" +
                "Write(Player, PId)\n" +
                "Write(Player, item.owner)\n";

        slwGraph.addNewTransaction(TransactionParser.parseTransaction(addListing));
        slwGraph.addNewTransaction(TransactionParser.parseTransaction(buyListing));


//        graph.show();
        System.out.println(slwGraph.isThereADeadlock() ? "There is a deadlock" : "There is no deadlock");


        List<List<List<TXNode>>> deadlockFreeTransactions =  slwGraph.generateAllDeadlockFreeTransactions();
        System.out.println("-------Original Chains-----------");
        slwGraph.printChains();
        System.out.println("------------------");
        for (List<List<TXNode>> transactionSet:
             deadlockFreeTransactions) {
            System.out.println("------------------");
            System.out.println("Score: " + slwGraph.getChainsScore(transactionSet));
            SLWGraph.printChains(transactionSet);
            System.out.println("------------------");
        }

    }
}
