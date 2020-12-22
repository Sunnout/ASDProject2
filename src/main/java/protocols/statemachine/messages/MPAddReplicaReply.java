package protocols.statemachine.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class MPAddReplicaReply extends ProtoMessage {

    public static final short MSG_ID = 205;

    private int instance;
    private byte[] state;
    private List<Host> membership;
    private Host currentLeader;

    public MPAddReplicaReply(int instance, byte[] state, List<Host> membership, Host currentLeader) {
        super(MSG_ID);
        this.instance = instance;
        this.state = state;
        this.membership = membership;
        this.currentLeader = currentLeader;
    }

    public int getInstance() {
        return this.instance;
    }

    public byte[] getState() {
        return this.state;
    }

    public List<Host> getMembership() {
        return membership;
    }

    public Host getCurrentLeader() {
        return currentLeader;
    }

    @Override
    public String toString() {
        return "CurrentStateReply{" +
                "instance=" + instance +
                "number of bytes=" + state.length +
                '}';
    }

    public static ISerializer<MPAddReplicaReply> serializer = new ISerializer<MPAddReplicaReply>() {
        @Override
        public void serialize(MPAddReplicaReply msg, ByteBuf out) throws IOException {
            out.writeInt(msg.instance);
            out.writeInt(msg.state.length);
            out.writeBytes(msg.state);
            out.writeInt(msg.membership.size());
            for(Host h: msg.membership)
                Host.serializer.serialize(h, out);
            Host.serializer.serialize(msg.currentLeader, out);
        }

        @Override
        public MPAddReplicaReply deserialize(ByteBuf in) throws IOException {
            int instance = in.readInt();
            int dataSize = in.readInt();
            byte[] state = new byte[dataSize];
            in.readBytes(state);

            int membershipSize = in.readInt();
            List<Host> membership = new LinkedList<>();

            for(int i = 0; i < membershipSize; i++){
                membership.add(Host.serializer.deserialize(in));
            }

            Host currentLeader = Host.serializer.deserialize(in);

            return new MPAddReplicaReply(instance, state, membership, currentLeader);
        }
    };
}
