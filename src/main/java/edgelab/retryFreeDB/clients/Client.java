package edgelab.retryFreeDB.clients;

import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.Map;

public abstract class Client {
    public static class TransactionResult {
        @Getter
        @Setter
        boolean success;

        @Getter
        @Setter
        Map<String, String> metrics;
        @Getter
        long start;

        @Setter
        @Getter
        String message;

        @Setter
        @Getter
        String txId;

        public TransactionResult() {
            this.success = false;
            this.start = System.nanoTime();
            this.message = "";
        }
    }
    public abstract void setServerConfig(Map<String, String> serverConfig);
    public abstract boolean readItem(List<String> itemsToGet);
    public abstract TransactionResult TPCC_payment(String warehouseId, String districtId, float paymentAmount, String customerWarehouseId, String customerDistrictId, String customerId);
    public abstract TransactionResult TPCC_newOrder(String warehouseId, String districtId, String customerId, String orderLineCount, String allLocals, int[] itemIds, int[] supplierWarehouseIds, int[] orderQuantities);
    public abstract TransactionResult addListing(String pId, String iId, double i, String txId);
    public abstract TransactionResult buyListing(String pId, String lId, String txId);
}
