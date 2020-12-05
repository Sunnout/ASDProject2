package protocols.statemachine.utils;

import io.netty.buffer.ByteBuf;
import protocols.app.utils.Operation;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

public class PaxosState {

    private Operation decision;
    private UUID opId;

    public PaxosState(Operation decision, UUID opId) {
        this.decision = decision;
        this.opId = opId;
    }

    public Operation getDecision() {
        return decision;
    }

    public void setDecision(Operation decision) {
        this.decision = decision;
    }

    public UUID getOpId() {
        return opId;
    }

    public static ISerializer<PaxosState> serializer = new ISerializer<PaxosState>() {
        @Override
        public void serialize(PaxosState paxos, ByteBuf out) throws IOException {
            byte[] d = paxos.decision.toByteArray();
            out.writeInt(d.length);
            out.writeBytes(d);
            out.writeLong(paxos.opId.getMostSignificantBits());
            out.writeLong(paxos.opId.getLeastSignificantBits());
        }

        @Override
        public PaxosState deserialize(ByteBuf in) throws IOException {
            byte[] bytes = new byte[in.readInt()];
            in.readBytes(bytes);
            long highBytes = in.readLong();
            long lowBytes = in.readLong();
            UUID opId = new UUID(highBytes, lowBytes);
            return new PaxosState(Operation.fromByteArray(bytes), opId);
        }
    };
}
