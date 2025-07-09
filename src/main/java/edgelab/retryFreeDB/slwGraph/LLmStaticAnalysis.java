package edgelab.retryFreeDB.slwGraph;

public class LLmStaticAnalysis {
    private static final SLWGraph slwGraph = new SLWGraph();
    private static final String LLM_OUTPUT_TRANSACTIONS = "[\n" +
            "  {\n" +
            "    \"transactionName\": \"newOrder\",\n" +
            "    \"inputs\": [\n" +
            "      \"warehouseId\",\n" +
            "      \"districtId\",\n" +
            "      \"customerId\",\n" +
            "      \"orderLineCount\",\n" +
            "      \"allLocals\",\n" +
            "      \"itemIDs\",\n" +
            "      \"supplierWarehouseIDs\",\n" +
            "      \"orderQuantities\",\n" +
            "      \"txId\"\n" +
            "    ],\n" +
            "    \"statements\": [\n" +
            "      {\n" +
            "        \"type\": \"exclusive_lock\",\n" +
            "        \"table\": \"customer\",\n" +
            "        \"keys\": [\n" +
            "          \"warehouseId\",\n" +
            "          \"districtId\",\n" +
            "          \"customerId\"\n" +
            "        ]\n" +
            "      },\n" +
            "      {\n" +
            "        \"type\": \"exclusive_lock\",\n" +
            "        \"table\": \"district\",\n" +
            "        \"keys\": [\n" +
            "          \"warehouseId\",\n" +
            "          \"districtId\"\n" +
            "        ]\n" +
            "      },\n" +
            "      {\n" +
            "        \"type\": \"shared_lock\",\n" +
            "        \"table\": \"warehouse\",\n" +
            "        \"keys\": [\n" +
            "          \"warehouseId\"\n" +
            "        ]\n" +
            "      },\n" +
            "      {\n" +
            "        \"type\": \"shared_lock\",\n" +
            "        \"table\": \"item\",\n" +
            "        \"keys\": [\n" +
            "          \"itemIDs\"\n" +
            "        ]\n" +
            "      },\n" +
            "      {\n" +
            "        \"type\": \"exclusive_lock\",\n" +
            "        \"table\": \"new_order\",\n" +
            "        \"keys\": [\n" +
            "          \"warehouseId\",\n" +
            "          \"districtId\",\n" +
            "          \"D.d_next_o_id\"\n" +
            "        ]\n" +
            "      },\n" +
            "      {\n" +
            "        \"type\": \"exclusive_lock\",\n" +
            "        \"table\": \"oorder\",\n" +
            "        \"keys\": [\n" +
            "          \"warehouseId\",\n" +
            "          \"districtId\",\n" +
            "          \"customerId\",\n" +
            "          \"orderLineCount\",\n" +
            "          \"allLocals\"\n" +
            "        ]\n" +
            "      },\n" +
            "      {\n" +
            "        \"type\": \"exclusive_lock\",\n" +
            "        \"table\": \"order_line\",\n" +
            "        \"keys\": [\n" +
            "          \"warehouseId\",\n" +
            "          \"districtId\",\n" +
            "          \"D.d_next_o_id\",\n" +
            "          \"itemIDs\",\n" +
            "          \"supplierWarehouseIDs\",\n" +
            "          \"orderQuantities\"\n" +
            "        ]\n" +
            "      },\n" +
            "      {\n" +
            "        \"type\": \"exclusive_lock\",\n" +
            "        \"table\": \"stock\",\n" +
            "        \"keys\": [\n" +
            "          \"supplierWarehouseIDs\",\n" +
            "          \"itemIDs\"\n" +
            "        ]\n" +
            "      },\n" +
            "      {\n" +
            "        \"type\": \"read\",\n" +
            "        \"table\": \"customer\",\n" +
            "        \"keys\": [\n" +
            "          \"warehouseId\",\n" +
            "          \"districtId\",\n" +
            "          \"customerId\"\n" +
            "        ],\n" +
            "        \"output\": \"C\",\n" +
            "        \"commutative\": false\n" +
            "      },\n" +
            "      {\n" +
            "        \"type\": \"read\",\n" +
            "        \"table\": \"warehouse\",\n" +
            "        \"keys\": [\n" +
            "          \"warehouseId\"\n" +
            "        ],\n" +
            "        \"output\": \"W\",\n" +
            "        \"commutative\": false\n" +
            "      },\n" +
            "      {\n" +
            "        \"type\": \"read\",\n" +
            "        \"table\": \"district\",\n" +
            "        \"keys\": [\n" +
            "          \"warehouseId\",\n" +
            "          \"districtId\"\n" +
            "        ],\n" +
            "        \"output\": \"D\",\n" +
            "        \"commutative\": false\n" +
            "      },\n" +
            "      {\n" +
            "        \"type\": \"write\",\n" +
            "        \"table\": \"district\",\n" +
            "        \"keys\": [\n" +
            "          \"warehouseId\",\n" +
            "          \"districtId\"\n" +
            "        ],\n" +
            "        \"commutative\": false\n" +
            "      },\n" +
            "      {\n" +
            "        \"type\": \"insert\",\n" +
            "        \"table\": \"oorder\",\n" +
            "        \"keys\": [\n" +
            "          \"warehouseId\",\n" +
            "          \"districtId\",\n" +
            "          \"customerId\",\n" +
            "          \"orderLineCount\",\n" +
            "          \"allLocals\"\n" +
            "        ],\n" +
            "        \"commutative\": true\n" +
            "      },\n" +
            "      {\n" +
            "        \"type\": \"unlock\",\n" +
            "        \"table\": \"oorder\",\n" +
            "        \"keys\": [\n" +
            "          \"warehouseId\",\n" +
            "          \"districtId\",\n" +
            "          \"customerId\",\n" +
            "          \"orderLineCount\",\n" +
            "          \"allLocals\"\n" +
            "        ]\n" +
            "      },\n" +
            "      {\n" +
            "        \"type\": \"insert\",\n" +
            "        \"table\": \"new_order\",\n" +
            "        \"keys\": [\n" +
            "          \"warehouseId\",\n" +
            "          \"districtId\",\n" +
            "          \"D.d_next_o_id\"\n" +
            "        ],\n" +
            "        \"commutative\": true\n" +
            "      },\n" +
            "      {\n" +
            "        \"type\": \"unlock\",\n" +
            "        \"table\": \"new_order\",\n" +
            "        \"keys\": [\n" +
            "          \"warehouseId\",\n" +
            "          \"districtId\",\n" +
            "          \"D.d_next_o_id\"\n" +
            "        ]\n" +
            "      },\n" +
            "      {\n" +
            "        \"type\": \"read\",\n" +
            "        \"table\": \"item\",\n" +
            "        \"keys\": [\n" +
            "          \"itemIDs\"\n" +
            "        ],\n" +
            "        \"output\": \"I\",\n" +
            "        \"commutative\": false\n" +
            "      },\n" +
            "      {\n" +
            "        \"type\": \"unlock\",\n" +
            "        \"table\": \"item\",\n" +
            "        \"keys\": [\n" +
            "          \"itemIDs\"\n" +
            "        ]\n" +
            "      },\n" +
            "      {\n" +
            "        \"type\": \"read\",\n" +
            "        \"table\": \"stock\",\n" +
            "        \"keys\": [\n" +
            "          \"supplierWarehouseIDs\",\n" +
            "          \"itemIDs\"\n" +
            "        ],\n" +
            "        \"output\": \"S\",\n" +
            "        \"commutative\": false\n" +
            "      },\n" +
            "      {\n" +
            "        \"type\": \"insert\",\n" +
            "        \"table\": \"order_line\",\n" +
            "        \"keys\": [\n" +
            "          \"warehouseId\",\n" +
            "          \"districtId\",\n" +
            "          \"D.d_next_o_id\",\n" +
            "          \"itemIDs\",\n" +
            "          \"supplierWarehouseIDs\",\n" +
            "          \"orderQuantities\"\n" +
            "        ],\n" +
            "        \"commutative\": true\n" +
            "      },\n" +
            "      {\n" +
            "        \"type\": \"unlock\",\n" +
            "        \"table\": \"order_line\",\n" +
            "        \"keys\": [\n" +
            "          \"warehouseId\",\n" +
            "          \"districtId\",\n" +
            "          \"D.d_next_o_id\",\n" +
            "          \"itemIDs\",\n" +
            "          \"supplierWarehouseIDs\",\n" +
            "          \"orderQuantities\"\n" +
            "        ]\n" +
            "      },\n" +
            "      {\n" +
            "        \"type\": \"write\",\n" +
            "        \"table\": \"stock\",\n" +
            "        \"keys\": [\n" +
            "          \"supplierWarehouseIDs\",\n" +
            "          \"itemIDs\"\n" +
            "        ],\n" +
            "        \"commutative\": false\n" +
            "      },\n" +
            "      {\n" +
            "        \"type\": \"unlock\",\n" +
            "        \"table\": \"stock\",\n" +
            "        \"keys\": [\n" +
            "          \"supplierWarehouseIDs\",\n" +
            "          \"itemIDs\"\n" +
            "        ]\n" +
            "      },\n" +
            "      {\n" +
            "        \"type\": \"unlock\",\n" +
            "        \"table\": \"customer\",\n" +
            "        \"keys\": [\n" +
            "          \"warehouseId\",\n" +
            "          \"districtId\",\n" +
            "          \"customerId\"\n" +
            "        ]\n" +
            "      },\n" +
            "      {\n" +
            "        \"type\": \"unlock\",\n" +
            "        \"table\": \"district\",\n" +
            "        \"keys\": [\n" +
            "          \"warehouseId\",\n" +
            "          \"districtId\"\n" +
            "        ]\n" +
            "      },\n" +
            "      {\n" +
            "        \"type\": \"unlock\",\n" +
            "        \"table\": \"warehouse\",\n" +
            "        \"keys\": [\n" +
            "          \"warehouseId\"\n" +
            "        ]\n" +
            "      }\n" +
            "    ]\n" +
            "  },\n" +
            "  {\n" +
            "    \"transactionName\": \"payment\",\n" +
            "    \"inputs\": [\n" +
            "      \"warehouseId\",\n" +
            "      \"districtId\",\n" +
            "      \"paymentAmount\",\n" +
            "      \"customerWarehouseId\",\n" +
            "      \"customerDistrictId\",\n" +
            "      \"customerId\",\n" +
            "      \"txId\"\n" +
            "    ],\n" +
            "    \"statements\": [\n" +
            "      {\n" +
            "        \"type\": \"exclusive_lock\",\n" +
            "        \"table\": \"customer\",\n" +
            "        \"keys\": [\n" +
            "          \"customerWarehouseId\",\n" +
            "          \"customerDistrictId\",\n" +
            "          \"customerId\"\n" +
            "        ]\n" +
            "      },\n" +
            "      {\n" +
            "        \"type\": \"exclusive_lock\",\n" +
            "        \"table\": \"district\",\n" +
            "        \"keys\": [\n" +
            "          \"warehouseId\",\n" +
            "          \"districtId\"\n" +
            "        ]\n" +
            "      },\n" +
            "      {\n" +
            "        \"type\": \"exclusive_lock\",\n" +
            "        \"table\": \"warehouse\",\n" +
            "        \"keys\": [\n" +
            "          \"warehouseId\"\n" +
            "        ]\n" +
            "      },\n" +
            "      {\n" +
            "        \"type\": \"exclusive_lock\",\n" +
            "        \"table\": \"history\",\n" +
            "        \"keys\": [\n" +
            "          \"warehouseId\",\n" +
            "          \"districtId\",\n" +
            "          \"customerWarehouseId\",\n" +
            "          \"customerDistrictId\",\n" +
            "          \"customerId\",\n" +
            "          \"paymentAmount\",\n" +
            "          \"W.w_name\",\n" +
            "          \"D.d_name\"\n" +
            "        ]\n" +
            "      },\n" +
            "      {\n" +
            "        \"type\": \"read\",\n" +
            "        \"table\": \"warehouse\",\n" +
            "        \"keys\": [\n" +
            "          \"warehouseId\"\n" +
            "        ],\n" +
            "        \"output\": \"W\",\n" +
            "        \"commutative\": false\n" +
            "      },\n" +
            "      {\n" +
            "        \"type\": \"write\",\n" +
            "        \"table\": \"warehouse\",\n" +
            "        \"keys\": [\n" +
            "          \"warehouseId\",\n" +
            "          \"paymentAmount\"\n" +
            "        ],\n" +
            "        \"commutative\": false\n" +
            "      },\n" +
            "      {\n" +
            "        \"type\": \"read\",\n" +
            "        \"table\": \"district\",\n" +
            "        \"keys\": [\n" +
            "          \"warehouseId\",\n" +
            "          \"districtId\"\n" +
            "        ],\n" +
            "        \"output\": \"D\",\n" +
            "        \"commutative\": false\n" +
            "      },\n" +
            "      {\n" +
            "        \"type\": \"write\",\n" +
            "        \"table\": \"district\",\n" +
            "        \"keys\": [\n" +
            "          \"warehouseId\",\n" +
            "          \"districtId\",\n" +
            "          \"paymentAmount\"\n" +
            "        ],\n" +
            "        \"commutative\": false\n" +
            "      },\n" +
            "      {\n" +
            "        \"type\": \"read\",\n" +
            "        \"table\": \"customer\",\n" +
            "        \"keys\": [\n" +
            "          \"customerWarehouseId\",\n" +
            "          \"customerDistrictId\",\n" +
            "          \"customerId\"\n" +
            "        ],\n" +
            "        \"output\": \"C\",\n" +
            "        \"commutative\": false\n" +
            "      },\n" +
            "      {\n" +
            "        \"type\": \"write\",\n" +
            "        \"table\": \"customer\",\n" +
            "        \"keys\": [\n" +
            "          \"customerWarehouseId\",\n" +
            "          \"customerDistrictId\",\n" +
            "          \"customerId\",\n" +
            "          \"paymentAmount\"\n" +
            "        ],\n" +
            "        \"commutative\": false\n" +
            "      },\n" +
            "      {\n" +
            "        \"type\": \"insert\",\n" +
            "        \"table\": \"history\",\n" +
            "        \"keys\": [\n" +
            "          \"warehouseId\",\n" +
            "          \"districtId\",\n" +
            "          \"customerWarehouseId\",\n" +
            "          \"customerDistrictId\",\n" +
            "          \"customerId\",\n" +
            "          \"paymentAmount\",\n" +
            "          \"W.w_name\",\n" +
            "          \"D.d_name\"\n" +
            "        ],\n" +
            "        \"commutative\": true\n" +
            "      },\n" +
            "      {\n" +
            "        \"type\": \"unlock\",\n" +
            "        \"table\": \"history\",\n" +
            "        \"keys\": [\n" +
            "          \"warehouseId\",\n" +
            "          \"districtId\",\n" +
            "          \"customerWarehouseId\",\n" +
            "          \"customerDistrictId\",\n" +
            "          \"customerId\",\n" +
            "          \"paymentAmount\",\n" +
            "          \"W.w_name\",\n" +
            "          \"D.d_name\"\n" +
            "        ]\n" +
            "      },\n" +
            "      {\n" +
            "        \"type\": \"unlock\",\n" +
            "        \"table\": \"customer\",\n" +
            "        \"keys\": [\n" +
            "          \"customerWarehouseId\",\n" +
            "          \"customerDistrictId\",\n" +
            "          \"customerId\"\n" +
            "        ]\n" +
            "      },\n" +
            "      {\n" +
            "        \"type\": \"unlock\",\n" +
            "        \"table\": \"district\",\n" +
            "        \"keys\": [\n" +
            "          \"warehouseId\",\n" +
            "          \"districtId\"\n" +
            "        ]\n" +
            "      },\n" +
            "      {\n" +
            "        \"type\": \"unlock\",\n" +
            "        \"table\": \"warehouse\",\n" +
            "        \"keys\": [\n" +
            "          \"warehouseId\"\n" +
            "        ]\n" +
            "      }\n" +
            "    ]\n" +
            "  }\n" +
            "]";
    public static void main(String[] args) {
        slwGraph.loadChainsFromJson(LLM_OUTPUT_TRANSACTIONS);
        System.out.println(slwGraph.isThereADeadlock() ? "There is a deadlock" : "There is no deadlock");
        System.out.println(slwGraph.isItSerializable() ? "Transactions are serializable" : "Transactions are NOT serializable");
        System.out.println("-------LLM Chains-----------");
        slwGraph.printChains();
        System.out.println("------------------");

    }
}
