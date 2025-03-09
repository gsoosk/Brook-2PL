package edgelab.retryFreeDB.slwGraph;

import lombok.Data;
import java.util.List;

@Data
public class TransactionRepresentation {


    @Data
    public class Operation {
        private String outputName;
        private boolean commutative;
        private boolean selfCommutative;
        public enum Type {
            WRITE, READ
        }
        private Type type;
        private String tableName;
        private String id;
        public Operation(String outputName, Type type, String tableName, String id) {
            this.outputName = outputName;
            this.type = type;
            this.tableName = tableName;
            this.id = id;
        }
    }

    /*
    * TX(Id1):
    * X = Read(Table1, Id1)
    * Write(Table2, X)
    *
    *
    * TX -> (List<Inputs>, List<Operations>)
    * */

    private List<String> inputs;
    private List<Operation> operations;
    public TransactionRepresentation(List<String> inputs, List<Operation> operations) {
        this.inputs = inputs;
        this.operations = operations;
    }

}
