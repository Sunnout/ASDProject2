package protocols.statemachine.messages;

import io.netty.buffer.ByteBuf;
import protocols.app.utils.Operation;
import protocols.statemachine.utils.OperationAndId;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

public class ProposeToLeaderMessage extends ProtoMessage {

    public static final short MSG_ID = 204;

    private List<OperationAndId> proposedOperations;

    public ProposeToLeaderMessage(List<OperationAndId> proposedOperations) {
        super(MSG_ID);
        this.proposedOperations = proposedOperations;
    }

    public List<OperationAndId> getProposedOperations() {
        return proposedOperations;
    }

    public static ISerializer<ProposeToLeaderMessage> serializer = new ISerializer<ProposeToLeaderMessage>() {
        @Override
        public void serialize(ProposeToLeaderMessage msg, ByteBuf out) throws IOException {
            byte[] op;
            UUID opId;
            out.writeInt(msg.proposedOperations.size());

            for(OperationAndId opnId: msg.proposedOperations) {
                op = opnId.getOperation().toByteArray();
                opId = opnId.getOpId();
                out.writeLong(opId.getMostSignificantBits());
                out.writeLong(opId.getLeastSignificantBits());
                out.writeInt(op.length);
                out.writeBytes(op);
            }
        }

        @Override
        public ProposeToLeaderMessage deserialize(ByteBuf in) throws IOException {
            int numberOperations = in.readInt();
            long highBytes;
            long lowBytes;
            UUID opId;
            byte[] op;
            List<OperationAndId> proposedOperations = new ArrayList<>();

            for(int i = 0; i < numberOperations; i++){
                highBytes = in.readLong();
                lowBytes = in.readLong();
                opId = new UUID(highBytes, lowBytes);
                op = new byte[in.readInt()];
                in.readBytes(op);
                OperationAndId opnId = new OperationAndId(Operation.fromByteArray(op), opId);
                proposedOperations.add(opnId);
            }

            return new ProposeToLeaderMessage(proposedOperations);
        }
    };
}