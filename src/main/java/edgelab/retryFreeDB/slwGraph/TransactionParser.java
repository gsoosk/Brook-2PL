package edgelab.retryFreeDB.slwGraph;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TransactionParser {

    /**
     * Parses the given TX program string and returns a TransactionRepresentation.
     *
     * Supported operations:
     * - Assignment: e.g. "L = Read(Listing, LId)" or "X = Write(Player, L.seller)"
     * - Non-assignment Read: e.g. "Read(Player, PId)" (output remains null)
     * - Insert: e.g. "Insert(Listing)" converts to a Write op with output=null and id=null
     * - Delete: e.g. "Delete(Listing, LId)" converts to a Write op with output=null and id="LId"
     * - Write: e.g. "Write(Item, L.seller)" converts to a Write op with id trimmed to the part before the dot
     *
     * For both assignment and non-assignment operations, if the id contains a dot,
     * only the part before the dot is kept.
     *
     * @param txProgram the transaction program as a multi-line string
     * @return a TransactionRepresentation instance populated with inputs and operations
     */
    public static TransactionRepresentation parseTransaction(String txProgram) {
        // Split the input into lines.
        String[] lines = txProgram.split("\\r?\\n");
        List<String> inputs = new ArrayList<>();
        // Each operation is stored as an array of 4 strings:
        // [outputName, opType, tableName, id]
        List<String[]> opInfos = new ArrayList<>();

        // Define regex patterns.
        Pattern txPattern = Pattern.compile("^TX\\(([^)]+)\\):\\s*$");
        // Assignment operation: e.g. "L = Read(Listing, LId)" or "X = Write(Player, L.seller)"
        Pattern assignmentOpPattern = Pattern.compile("^(\\w+)\\s*=\\s*(\\w+)\\((\\w+),\\s*([\\w]+(?:\\.[\\w]+)?)\\)\\s*$");
        // Insert operation: e.g. "Insert(Listing)"
        Pattern insertOpPattern = Pattern.compile("^Insert\\((\\w+)\\)\\s*$");
        // Delete operation: e.g. "Delete(Listing, LId)"
        Pattern deleteOpPattern = Pattern.compile("^Delete\\((\\w+),\\s*(\\w+)\\)\\s*$");
        // Non-assignment operation (for both Read and Write) that allows a dot in the id.
        Pattern noAssignmentOpPattern = Pattern.compile("^(\\w+)\\((\\w+),\\s*([\\w]+(?:\\.[\\w]+)?)\\)\\s*$");

        for (String line : lines) {
            line = line.trim();
            if (line.isEmpty()) {
                continue;
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
                // Operation with assignment.
                String outputName = assignmentMatcher.group(1).trim();
                String opTypeStr = assignmentMatcher.group(2).trim();
                String tableName = assignmentMatcher.group(3).trim();
                String id = assignmentMatcher.group(4).trim();
                opInfos.add(new String[] { outputName, opTypeStr, tableName, id });
            } else if (insertMatcher.matches()) {
                // Insert operation: converted to a Write operation with no output and no id.
                String tableName = insertMatcher.group(1).trim();
                opInfos.add(new String[] { null, "Write", tableName, null });
            } else if (deleteMatcher.matches()) {
                // Delete operation: converted to a Write operation with no output.
                String tableName = deleteMatcher.group(1).trim();
                String id = deleteMatcher.group(2).trim();
                opInfos.add(new String[] { null, "Write", tableName, id });
            } else if (noAssignmentMatcher.matches()) {
                // Non-assignment operation.
                String opTypeStr = noAssignmentMatcher.group(1).trim();
                String tableName = noAssignmentMatcher.group(2).trim();
                String id = noAssignmentMatcher.group(3).trim();
                opInfos.add(new String[] { null, opTypeStr, tableName, id });
            }
            // Lines not matching any pattern are ignored.
        }

        // Create a TransactionRepresentation instance.
        TransactionRepresentation tx = new TransactionRepresentation(inputs, new ArrayList<>());

        // Process each operation info.
        for (String[] opInfo : opInfos) {
            String outputName = opInfo[0]; // may be null (for non-assignment)
            String opTypeStr = opInfo[1];
            String tableName = opInfo[2];
            String id = opInfo[3];

            // For every operation, if the id contains a dot, only keep the part before the dot.
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
            tx.getOperations().add(op);
        }

        return tx;
    }

    // Example usage
    public static void main(String[] args) {
        String program = "buyListing(PId, LId):\n" +
                "listing = Read(Listing, LId)\n" +
                "buyer = Read(Player, PId)\n" +
                "item = Read(Item, listing.item)\n" +
                "seller = Read(Player, listing.seller)\n" +
                "Delete(Listing, LId)\n" +
                "Write(Item, listing.item)\n" +
                "Write(Player, PId)\n" +
                "Write(Player, listing.seller)\n";
        TransactionRepresentation tx = parseTransaction(program);

        // Print the parsed inputs and operations.
        System.out.println("Inputs: " + tx.getInputs());
        System.out.println("Operations:");
        for (TransactionRepresentation.Operation op : tx.getOperations()) {
            System.out.println("Output: " + op.getOutputName() +
                    ", Type: " + op.getType() +
                    ", Table: " + op.getTableName() +
                    ", ID: " + op.getId());
        }
    }
}
