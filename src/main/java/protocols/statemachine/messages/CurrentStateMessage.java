package protocols.statemachine.messages;

import io.netty.buffer.ByteBuf;
import protocols.statemachine.utils.PaxosState;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class CurrentStateMessage extends ProtoMessage {

    public final static short MSG_ID = 201;

    private final int nextInstance;
    private final List<Host> membership;
    private final List<PaxosState> previousPaxos;

    public CurrentStateMessage(List<Host> membership, int nextInstance, List<PaxosState> previousPaxos) {
        super(MSG_ID);
        this.membership = membership;
        this.nextInstance = nextInstance;
        this.previousPaxos = previousPaxos;
    }

    public List<Host> getMembership() {
        return membership;
    }

    public int getNextInstance() {
        return nextInstance;
    }

    public List<PaxosState> getPreviousPaxos() {
        return previousPaxos;
    }

    public static ISerializer<CurrentStateMessage> serializer = new ISerializer<CurrentStateMessage>() {
        @Override
        public void serialize(CurrentStateMessage msg, ByteBuf out) throws IOException {
            out.writeInt(msg.nextInstance);
            out.writeInt(msg.membership.size());
            for (Host h : msg.membership)
                Host.serializer.serialize(h, out);

            out.writeInt(msg.previousPaxos.size());
            for (PaxosState p : msg.previousPaxos)
                PaxosState.serializer.serialize(p, out);
        }

        @Override
        public CurrentStateMessage deserialize(ByteBuf in) throws IOException {
            int nextInstance = in.readInt();
            int size = in.readInt();
            List<Host> membership = new LinkedList<>();
            for(int i = 0; i < size; i++)
                membership.add(Host.serializer.deserialize(in));

            size = in.readInt();
            List<PaxosState> previousPaxos = new LinkedList<>();
            for(int i = 0; i < size; i++)
                previousPaxos.add(PaxosState.serializer.deserialize(in));

            return new CurrentStateMessage(membership, nextInstance, previousPaxos);
        }
    };

}
