package protocols.agreement.messages;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

public class PrepareMessage  extends ProtoMessage {
    public final static short MSG_ID = 401;

    private int sn;


    public PrepareMessage(int sn) {
        super(MSG_ID);
        this.sn = sn;
    }

    public int getSn() {
        return sn;
    }


    public static ISerializer<PrepareMessage> serializer = new ISerializer<PrepareMessage>() {
        @Override
        public void serialize(PrepareMessage msg, ByteBuf out) {
            out.writeInt(msg.getSn());
        }

        @Override
        public PrepareMessage deserialize(ByteBuf in) {
            return new PrepareMessage(in.readInt());
        }
    };


}
