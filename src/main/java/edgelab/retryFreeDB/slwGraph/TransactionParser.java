package edgelab.retryFreeDB.slwGraph;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


public class TransactionParser {


    /**
     * Parses the given TX program string and returns a TransactionRepresentation.
     * It supports optional annotations at the end of each line (e.g., "// commutative").
     * If the annotation equals "commutative" (ignoring case), the corresponding operation is marked as commutative.
     *
     * @param txProgram the transaction program as a multi-line string
     * @return a TransactionRepresentation instance populated with inputs and operations
     */
    public static TransactionRepresentation parseTransaction(String txProgram) {
        // Split the input into lines.
        String[] lines = txProgram.split("\\r?\\n");
        List<String> inputs = new ArrayList<>();
        // Each operation is stored as an array of 5 strings:
        // [outputName, opType, tableName, id, annotation]
        List<String[]> opInfos = new ArrayList<>();

        // Define regex patterns.
        Pattern txPattern = Pattern.compile("^TX\\(([^)]+)\\):\\s*$");
        Pattern assignmentOpPattern = Pattern.compile("^(\\w+)\\s*=\\s*(\\w+)\\((\\w+),\\s*([\\w]+(?:\\.[\\w]+)?)\\)\\s*$");
        Pattern insertOpPattern = Pattern.compile("^Insert\\((\\w+)\\)\\s*$");
        Pattern deleteOpPattern = Pattern.compile("^Delete\\((\\w+),\\s*(\\w+)\\)\\s*$");
        Pattern noAssignmentOpPattern = Pattern.compile("^(\\w+)\\((\\w+),\\s*([\\w]+(?:\\.[\\w]+)?)\\)\\s*$");

        for (String line : lines) {
            line = line.trim();
            if (line.isEmpty()) continue;

            // Extract annotation if present.
            String annotation = null;
            if (line.contains("//")) {
                String[] parts = line.split("//", 2);
                line = parts[0].trim();
                annotation = parts[1].trim();
            }

            Matcher txMatcher = txPattern.matcher(line);
            Matcher assignmentMatcher = assignmentOpPattern.matcher(line);
            Matcher insertMatcher = insertOpPattern.matcher(line);
            Matcher deleteMatcher = deleteOpPattern.matcher(line);
            Matcher noAssignmentMatcher = noAssignmentOpPattern.matcher(line);

            if (txMatcher.matches()) {
                // Header line: extract comma-separated inputs.
                String inputStr = txMatcher.group(1).trim();
                String[] inParts = inputStr.split(",");
                for (String in : inParts) {
                    inputs.add(in.trim());
                }
            } else if (assignmentMatcher.matches()) {
                String outputName = assignmentMatcher.group(1).trim();
                String opTypeStr = assignmentMatcher.group(2).trim();
                String tableName = assignmentMatcher.group(3).trim();
                String id = assignmentMatcher.group(4).trim();
                opInfos.add(new String[] { outputName, opTypeStr, tableName, id, annotation });
            } else if (insertMatcher.matches()) {
                String tableName = insertMatcher.group(1).trim();
                opInfos.add(new String[] { null, "Write", tableName, null, annotation });
            } else if (deleteMatcher.matches()) {
                String tableName = deleteMatcher.group(1).trim();
                String id = deleteMatcher.group(2).trim();
                opInfos.add(new String[] { null, "Write", tableName, id, annotation });
            } else if (noAssignmentMatcher.matches()) {
                String opTypeStr = noAssignmentMatcher.group(1).trim();
                String tableName = noAssignmentMatcher.group(2).trim();
                String id = noAssignmentMatcher.group(3).trim();
                opInfos.add(new String[] { null, opTypeStr, tableName, id, annotation });
            }
        }

        TransactionRepresentation tx = new TransactionRepresentation(inputs, new ArrayList<>());

        // Process each operation info.
        for (String[] opInfo : opInfos) {
            String outputName = opInfo[0]; // may be null
            String opTypeStr = opInfo[1];
            String tableName = opInfo[2];
            String id = opInfo[3];
            String opAnnotation = opInfo[4];

            // Remove dot from id if present (for all operations).
            if (id != null && id.contains(".")) {
                id = id.substring(0, id.indexOf('.'));
            }

            TransactionRepresentation.Operation.Type opType;
            if (opTypeStr.equalsIgnoreCase("Read")) {
                opType = TransactionRepresentation.Operation.Type.READ;
            } else if (opTypeStr.equalsIgnoreCase("Write")) {
                opType = TransactionRepresentation.Operation.Type.WRITE;
            } else {
                throw new IllegalArgumentException("Unknown operation type: " + opTypeStr);
            }

            // Create the operation.
            TransactionRepresentation.Operation op = tx.new Operation(outputName, opType, tableName, id);
            // Set the commutative flag if annotation equals "commutative"
            boolean commutative = opAnnotation != null && opAnnotation.equalsIgnoreCase("commutative");
            boolean selfCommutative =  opAnnotation != null && opAnnotation.equalsIgnoreCase("self-commutative");
            op.setSelfCommutative(selfCommutative);
            op.setCommutative(commutative);
            tx.getOperations().add(op);
        }

        return tx;
    }

    /**
     * Parses a transaction described in JSON and returns a TransactionRepresentation.
     * Expected JSON schema:
     * {
     *   "transactionName": "…",       // optional, currently ignored
     *   "inputs": ["input1", "input2"],
     *   "statements": [
     *     {
     *       "type": "read",           // "read" | "write" | "delete" | "insert"
     *       "table": "tableName",
     *       "key": "key",             // optional for insert
     *       "output": "outputName",   // only for reads
     *       "commutative": false
     *     },
     *     …
     *   ]
     * }
     *
     * @param jsonTx the transaction JSON string
     * @return a TransactionRepresentation populated with inputs & operations
     * @throws JSONException if the JSON is malformed
     */
    public static TransactionRepresentation parseTransactionFromJson(String jsonTx) throws JSONException {
        JSONObject root = new JSONObject(jsonTx);

        // 1) inputs
        JSONArray inArr = root.getJSONArray("inputs");
        List<String> inputs = new ArrayList<>(inArr.length());
        for (int i = 0; i < inArr.length(); i++) {
            inputs.add(inArr.getString(i));
        }

        // create transaction
        TransactionRepresentation tx = new TransactionRepresentation(inputs, new ArrayList<>());

        // 2) statements
        JSONArray stmts = root.getJSONArray("statements");
        for (int i = 0; i < stmts.length(); i++) {
            JSONObject stmt = stmts.getJSONObject(i);
            String type       = stmt.getString("type");
            String tableName  = stmt.getString("table");
            String key        = stmt.optString("key", null);
            String outputName = stmt.optString("output", null);
            boolean comm      = stmt.optBoolean("commutative", false);

            // strip dot‐suffix if any
            if (key != null && key.contains(".")) {
                key = key.substring(0, key.indexOf('.'));
            }

            // map to our enum
            TransactionRepresentation.Operation.Type opType;
            switch (type.toLowerCase()) {
                case "read":   opType = TransactionRepresentation.Operation.Type.READ;  break;
                case "write":
                case "delete":
                case "insert":
                    opType = TransactionRepresentation.Operation.Type.WRITE;
                    break;
                default:
                    throw new IllegalArgumentException("Unknown statement type: " + type);
            }

            // build operation
            TransactionRepresentation.Operation op =
                    tx.new Operation(outputName, opType, tableName, key);
            op.setCommutative(comm);
            // if you ever want a self‐comm flag, you could check a separate JSON field
            tx.getOperations().add(op);
        }

        return tx;
    }


    public static void main(String[] args) {
        String program = "TX(warehouseId, districtId, paymentAmount, customerWarehouseId, customerDistrictId, customerId, txId):\n" +
                "W = Read(warehouse, warehouseId)\n" +
                "Write(warehouse, warehouseId)\n" +
                "D = Read(district, warehouseId_districtId)\n" +
                "Write(district, warehouseId_districtId)\n" +
                "C = Read(customer, customerWarehouseId_customerDistrictId_customerId)\n" +
                "Write(customer, customerWarehouseId_customerDistrictId_customerId)\n" +
                "Insert(history) // commutative";
        TransactionRepresentation tx = parseTransaction(program);
        System.out.println("Inputs: " + tx.getInputs());
        System.out.println("Operations:");
        for (TransactionRepresentation.Operation op : tx.getOperations()) {
            System.out.println("Output: " + op.getOutputName() +
                    ", Type: " + op.getType() +
                    ", Table: " + op.getTableName() +
                    ", ID: " + op.getId() +
                    ", Commutative: " + op.isCommutative());
        }


        String programJson = "  {\n" +
                "    \"transactionName\": \"payment\",\n" +
                "    \"inputs\": [\"warehouseId\", \"districtId\", \"paymentAmount\", \"customerWarehouseId\", \"customerDistrictId\", \"customerId\", \"txId\"],\n" +
                "    \"statements\": [\n" +
                "      {\n" +
                "        \"type\": \"read\",\n" +
                "        \"table\": \"warehouse\",\n" +
                "        \"key\": \"warehouseId\",\n" +
                "        \"output\": \"W\",\n" +
                "        \"commutative\": false\n" +
                "      },\n" +
                "      {\n" +
                "        \"type\": \"write\",\n" +
                "        \"table\": \"warehouse\",\n" +
                "        \"key\": \"warehouseId, paymentAmount\",\n" +
                "        \"commutative\": false\n" +
                "      },\n" +
                "      {\n" +
                "        \"type\": \"read\",\n" +
                "        \"table\": \"district\",\n" +
                "        \"key\": \"warehouseId, districtId\",\n" +
                "        \"output\": \"D\",\n" +
                "        \"commutative\": false\n" +
                "      },\n" +
                "      {\n" +
                "        \"type\": \"write\",\n" +
                "        \"table\": \"district\",\n" +
                "        \"key\": \"warehouseId, districtId, paymentAmount\",\n" +
                "        \"commutative\": false\n" +
                "      },\n" +
                "      {\n" +
                "        \"type\": \"read\",\n" +
                "        \"table\": \"customer\",\n" +
                "        \"key\": \"customerWarehouseId, customerDistrictId, customerId\",\n" +
                "        \"output\": \"C\",\n" +
                "        \"commutative\": false\n" +
                "      },\n" +
                "      {\n" +
                "        \"type\": \"write\",\n" +
                "        \"table\": \"customer\",\n" +
                "        \"key\": \"customerWarehouseId, customerDistrictId, customerId, paymentAmount\",\n" +
                "        \"commutative\": false\n" +
                "      },\n" +
                "      {\n" +
                "        \"type\": \"insert\",\n" +
                "        \"table\": \"history\",\n" +
                "        \"key\": \"warehouseId, districtId, customerWarehouseId, customerDistrictId, customerId, paymentAmount, W.w_name, D.d_name\",\n" +
                "        \"commutative\": true\n" +
                "      }\n" +
                "    ]\n" +
                "  }";
        tx = parseTransactionFromJson(programJson);
        System.out.println("Inputs: " + tx.getInputs());
        System.out.println("Operations:");
        for (TransactionRepresentation.Operation op : tx.getOperations()) {
            System.out.println("Output: " + op.getOutputName() +
                    ", Type: " + op.getType() +
                    ", Table: " + op.getTableName() +
                    ", ID: " + op.getId() +
                    ", Commutative: " + op.isCommutative());
        }
    }



}
