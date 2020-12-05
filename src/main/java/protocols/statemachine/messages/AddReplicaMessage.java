package protocols.statemachine.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

import java.io.IOException;

public class AddReplicaMessage extends ProtoMessage {

    public final static short MSG_ID = 202;

    private final Host newHost;

    public AddReplicaMessage(Host newHost) {
        super(MSG_ID);
        this.newHost = newHost;
    }

    public Host getNewHost() {
        return newHost;
    }

    public static ISerializer<AddReplicaMessage> serializer = new ISerializer<AddReplicaMessage>() {
        @Override
        public void serialize(AddReplicaMessage addReplica, ByteBuf out) throws IOException {
            Host.serializer.serialize(addReplica.getNewHost(), out);
        }

        @Override
        public AddReplicaMessage deserialize(ByteBuf in) throws IOException {
            return new AddReplicaMessage(Host.serializer.deserialize(in));
        }
    };
}
