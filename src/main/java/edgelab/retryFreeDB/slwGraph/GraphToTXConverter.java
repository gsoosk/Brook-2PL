package edgelab.retryFreeDB.slwGraph;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GraphToTXConverter {

    /**
     * Converts a graph string (with nodes separated by "->") into a TX program string.
     * It supports the following token formats:
     *   - Lock tokens: "Lock (lockType) Table <[ID]>"
     *         If the captured ID is "null", then it is interpreted as an Insert (an Insert operation is added after a Lock).
     *   - Unlock tokens: "Unlock [table1, table2, ...]"
     *   - Read tokens: "Read Table:<ID>"
     *   - Write tokens: "Write Table:<ID>"
     *   - Insert tokens: "Insert(Table)" — if encountered outside a Lock, an exclusive lock is added before the Insert.
     *   - Delete tokens: "Delete(Table, ID)"
     *   - No-assignment operations (if any) are also supported.
     *
     * The TX header is built using any non‑null IDs encountered (in order of first appearance).
     *
     * @param graphString the input graph notation string.
     * @return a TX program string in the DSL.
     */
    public static String convertGraphStringToTX(String graphString) {
        // Split the input string by "->"
        String[] tokens = graphString.split("->");
        List<Operation> ops = new ArrayList<>();
        // Use a LinkedHashSet to preserve the order of first appearance for header parameters.
        Set<String> headerParams = new LinkedHashSet<>();

        // Define regex patterns.
        // New lock pattern: Lock (lockType) Table <[ID]>
        Pattern lockPattern = Pattern.compile("^Lock\\s*\\(([^)]+)\\)\\s+(\\w+)\\s+<\\[(.*?)\\]>$", Pattern.CASE_INSENSITIVE);
        // New unlock pattern: Unlock [table1, table2, ...]
        Pattern unlockPattern = Pattern.compile("^Unlock\\s+\\[([^\\]]+)\\]$", Pattern.CASE_INSENSITIVE);
        Pattern readPattern = Pattern.compile("^Read\\s+(\\w+):<([^>]+)>$", Pattern.CASE_INSENSITIVE);
        Pattern writePattern = Pattern.compile("^Write\\s+(\\w+):<([^>]+)>$", Pattern.CASE_INSENSITIVE);
        Pattern insertPattern = Pattern.compile("^Insert\\((\\w+)\\)$", Pattern.CASE_INSENSITIVE);
        Pattern deletePattern = Pattern.compile("^Delete\\((\\w+),\\s*([^\\)]+)\\)$", Pattern.CASE_INSENSITIVE);
        Pattern noAssignPattern = Pattern.compile("^(\\w+)\\((\\w+),\\s*([^\\)]+)\\)$", Pattern.CASE_INSENSITIVE);

        for (String token : tokens) {
            token = token.trim();
            // Process Lock tokens.
            Matcher m = lockPattern.matcher(token);
            if (m.find()) {
                String lockType = m.group(1).trim();
                String table = m.group(2).trim();
                String id = m.group(3).trim();
                if (id.equalsIgnoreCase("null")) {
                    // For a lock with null id, produce a Lock operation and then an Insert.
                    ops.add(new Operation("Lock", lockType, table, "null"));
                    ops.add(new Operation("Insert", table, null));
                } else {
                    headerParams.add(id);
                    ops.add(new Operation("Lock", lockType, table, id));
                }
                continue;
            }
            // Process Unlock tokens.
            m = unlockPattern.matcher(token);
            if (m.find()) {
                String tablesStr = m.group(1).trim();
                // Split the contents on comma and trim each token.
                String[] tables = tablesStr.split(",");
                List<String> tableList = new ArrayList<>();
                for (String t : tables) {
                    tableList.add(t.trim());
                }
                // Combine into a comma-separated string.
                String joinedTables = String.join(", ", tableList);
                ops.add(new Operation("Unlock", joinedTables, null));
                continue;
            }
            // Process Read tokens.
            m = readPattern.matcher(token);
            if (m.find()) {
                String table = m.group(1).trim();
                String id = m.group(2).trim();
                if (!id.equalsIgnoreCase("null")) {
                    headerParams.add(id);
                }
                ops.add(new Operation("Read", table, id));
                continue;
            }
            // Process Write tokens.
            m = writePattern.matcher(token);
            if (m.find()) {
                String table = m.group(1).trim();
                String id = m.group(2).trim();
                if (!id.equalsIgnoreCase("null")) {
                    headerParams.add(id);
                }
                ops.add(new Operation("Write", table, id));
                continue;
            }
            // Process Insert tokens.
            m = insertPattern.matcher(token);
            if (m.find()) {
                String table = m.group(1).trim();
                // Add an exclusive lock before the insert.
                ops.add(new Operation("Lock", "exclusive", table, "null"));
                ops.add(new Operation("Insert", table, null));
                continue;
            }
            // Process Delete tokens.
            m = deletePattern.matcher(token);
            if (m.find()) {
                String table = m.group(1).trim();
                String id = m.group(2).trim();
                if (!id.equalsIgnoreCase("null")) {
                    headerParams.add(id);
                }
                ops.add(new Operation("Delete", table, id));
                continue;
            }
            // Process no-assignment operations.
            m = noAssignPattern.matcher(token);
            if (m.find()) {
                String opType = m.group(1).trim();
                String table = m.group(2).trim();
                String id = m.group(3).trim();
                if (!id.equalsIgnoreCase("null")) {
                    headerParams.add(id);
                }
                ops.add(new Operation(opType, table, id));
                continue;
            }
            // Unrecognized tokens are skipped.
        }

        // Build the TX header.
        StringBuilder sb = new StringBuilder();
        sb.append("TX(");
        boolean first = true;
        for (String param : headerParams) {
            if (!first) {
                sb.append(", ");
            }
            sb.append(param);
            first = false;
        }
        sb.append("):\n");

        // Build the TX program lines.
        for (Operation op : ops) {
            switch (op.type.toLowerCase()) {
                case "lock":
                    // Output as: Lock(lockType, Table, id)
                    sb.append("Lock(")
                            .append(op.lockType)
                            .append(", ")
                            .append(op.table)
                            .append(", ")
                            .append(op.id)
                            .append(")");
                    break;
                case "unlock":
                    // Output as: Unlock(table1, table2, ...)
                    sb.append("Unlock(")
                            .append(op.table)
                            .append(")");
                    break;
                case "read":
                    sb.append("Read(")
                            .append(op.table)
                            .append(", ")
                            .append(op.id)
                            .append(")");
                    break;
                case "write":
                    sb.append("Write(")
                            .append(op.table)
                            .append(", ")
                            .append(op.id)
                            .append(")");
                    break;
                case "insert":
                    sb.append("Insert(")
                            .append(op.table)
                            .append(")");
                    break;
                case "delete":
                    sb.append("Delete(")
                            .append(op.table)
                            .append(", ")
                            .append(op.id)
                            .append(")");
                    break;
                default:
                    sb.append(op.type)
                            .append("(")
                            .append(op.table);
                    if (op.id != null) {
                        sb.append(", ")
                                .append(op.id);
                    }
                    sb.append(")");
            }
            sb.append("\n");
        }
        return sb.toString();
    }

    // Private helper class to represent an operation.
    private static class Operation {
        String type;
        // For Lock operations, store the lock type.
        String lockType;
        // For Unlock tokens that support multiple tables, we store the table field as a comma-separated string.
        String table;
        String id;

        // Constructor for operations with a lock type (Lock).
        Operation(String type, String lockType, String table, String id) {
            this.type = type;
            this.lockType = lockType;
            this.table = table;
            this.id = id;
        }

        // Constructor for operations without a lock type.
        Operation(String type, String table, String id) {
            this(type, null, table, id);
        }
    }

    // Example usage.
    public static void main(String[] args) {
        String input1 = "Lock (exclusive) Listing <[null]>  -> Lock (shared) Item <[IId]>  -> Read Item:<IId> -> Lock (shared) Player <[PId]>  -> Unlock [Item] -> Read Player:<PId> -> Unlock [Player] -> Write Listing:<null> -> Unlock [Listing]";
        String input2 = "Lock (exclusive) Listing <[LId]>  -> Read Listing:<LId> -> Lock (exclusive) Item <[listing]>  -> Read Item:<listing> -> Lock (exclusive) Player <[item, PId]>  -> Read Player:<PId> -> Read Player:<item> -> Write Listing:<LId> -> Write Item:<listing> -> Unlock [Item] -> Write Player:<PId> -> Write Player:<item> -> Unlock [Player] -> Unlock [Listing]";
        String input3 = "Lock (shared) X <[x1]> -> Unlock [item, oorder, district, order_line, warehouse, stock, new_order, customer]";

        System.out.println("Converted TX (Input 1):\n" + convertGraphStringToTX(input1));
        System.out.println("Converted TX (Input 2):\n" + convertGraphStringToTX(input2));
        System.out.println("Converted TX (Input 3):\n" + convertGraphStringToTX(input3));
    }
}
