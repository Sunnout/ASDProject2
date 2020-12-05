package protocols.statemachine.utils;

import io.netty.buffer.ByteBuf;
import protocols.app.utils.Operation;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class PaxosState {

    private Operation decision;

    public PaxosState(Operation decision) {
        this.decision = decision;
    }

    public Operation getDecision() {
        return decision;
    }

    public void setDecision(Operation decision) {
        this.decision = decision;
    }

    public static ISerializer<PaxosState> serializer = new ISerializer<PaxosState>() {
        @Override
        public void serialize(PaxosState paxos, ByteBuf out) throws IOException {
            byte[] d = paxos.decision.toByteArray();
            out.writeInt(d.length);
            out.writeBytes(d);
        }

        @Override
        public PaxosState deserialize(ByteBuf in) throws IOException {
            byte[] bytes = new byte[in.readInt()];
            in.readBytes(bytes);
            return new PaxosState(Operation.fromByteArray(bytes));
        }
    };
}
