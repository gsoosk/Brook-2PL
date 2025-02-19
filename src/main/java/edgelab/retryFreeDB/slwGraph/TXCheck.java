package edgelab.retryFreeDB.slwGraph;

import edgelab.retryFreeDB.slwGraph.nodes.ReadNode;
import edgelab.retryFreeDB.slwGraph.nodes.WriteNode;

import java.util.List;

public class TXCheck {
    private static final SLWGraph graph = new SLWGraph();
    public static void main(String args[])
    {
//        addBuyListings();
        addBuyListingsLockItemsMovedToFront();
        addAddListing();
        addIncreaseCash();

        graph.show();
        System.out.println(graph.isThereADeadlock() ? "There is a deadlock" : "There is no deadlock");

    }

    private static void addIncreaseCash() {
        graph.addNewTransaction(List.of(
                new ReadNode("Players"),
                new WriteNode("Players")
        ));
        graph.addNewTransaction(List.of(
                new ReadNode("Players"),
                new WriteNode("Players")
        ));
    }

    private static void addBuyListingsLockItemsMovedToFront() {
        graph.addNewTransaction(List.of(
                new ReadNode("Items"), // To lock Items
                new ReadNode("Players"),
                new ReadNode("Listings"),
                new WriteNode("Listings"),
                new WriteNode("Items"),
                new WriteNode("Players")
        ));
        graph.addNewTransaction(List.of(
                new ReadNode("Items"), // To lock Items
                new ReadNode("Players"),
                new ReadNode("Listings"),
                new WriteNode("Listings"),
                new WriteNode("Items"),
                new WriteNode("Players")
        ));
    }

    private static void addAddListing() {
        graph.addNewTransaction(List.of(
                new ReadNode("Items"),
                new ReadNode("Players"),
                new WriteNode("Items"),
                new WriteNode("Listings"),
                new WriteNode("Players")
        ));
        graph.addNewTransaction(List.of(
                new ReadNode("Items"),
                new ReadNode("Players"),
                new WriteNode("Items"),
                new WriteNode("Listings"),
                new WriteNode("Players")
        ));
    }

    private static void addBuyListings() {
        graph.addNewTransaction(List.of(
                new ReadNode("Players"),
                new ReadNode("Listings"),
                new WriteNode("Listings"),
                new WriteNode("Items"),
                new WriteNode("Players")
        ));
        graph.addNewTransaction(List.of(
                new ReadNode("Players"),
                new ReadNode("Listings"),
                new WriteNode("Listings"),
                new WriteNode("Items"),
                new WriteNode("Players")
        ));

    }
}
