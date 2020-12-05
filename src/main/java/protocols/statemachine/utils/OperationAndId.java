package protocols.statemachine.utils;

import protocols.app.utils.Operation;

import java.util.UUID;

public class OperationAndId {

    private Operation operation;
    private UUID opId;

    public OperationAndId(Operation operation, UUID opId) {
        this.operation = operation;
        this.opId = opId;
    }

    public Operation getOperation() {
        return operation;
    }

    public UUID getOpId() {
        return opId;
    }
}
