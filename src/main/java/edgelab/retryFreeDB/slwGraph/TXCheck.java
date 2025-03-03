package edgelab.retryFreeDB.slwGraph;

public class TXCheck {
    private static final SLWGraph graph = new SLWGraph();
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
                "seller = Read(Player, listing.seller)\n" +
                "Delete(Listing, LId)\n" +
                "Write(Item, listing.item)\n" +
                "Write(Player, PId)\n" +
                "Write(Player, listing.seller)\n";

        graph.addNewTransaction(TransactionParser.parseTransaction(addListing));
        graph.addNewTransaction(TransactionParser.parseTransaction(buyListing));

//        graph.show();
        System.out.println(graph.isThereADeadlock() ? "There is a deadlock" : "There is no deadlock");

    }
}
