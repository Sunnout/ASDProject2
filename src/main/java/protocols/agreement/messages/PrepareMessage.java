package protocols.agreement.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

public class PrepareMessage  extends ProtoMessage {
    public final static short MSG_ID = 401;

    private final int sn;
    private final int instance;

    public PrepareMessage(int sn, int instance) {
        super(MSG_ID);
        this.sn = sn;
        this.instance = instance;
    }

    public int getSn() {
        return sn;
    }

    public int getInstance() {
        return instance;
    }

    public static ISerializer<PrepareMessage> serializer = new ISerializer<PrepareMessage>() {
        @Override
        public void serialize(PrepareMessage msg, ByteBuf out) {
            out.writeInt(msg.getSn());
            out.writeInt(msg.getInstance());

        }

        @Override
        public PrepareMessage deserialize(ByteBuf in) {
            return new PrepareMessage(in.readInt(),in.readInt());
        }
    };


}
