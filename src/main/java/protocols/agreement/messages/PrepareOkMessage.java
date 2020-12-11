package protocols.agreement.messages;

import io.netty.buffer.ByteBuf;
import org.apache.commons.codec.binary.Hex;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.util.UUID;

public class PrepareOkMessage  extends ProtoMessage {
    public final static short MSG_ID = 402;

    private final UUID opId;
    private final int instance;
    private final byte[] op;
    private final int highestAccepted;
    private final int sn;
    // TODO meter aqui o nosso seqNumber atual

    public PrepareOkMessage(int instance, UUID opId, byte[] op, int highestAccepted, int sn) {
        super(MSG_ID);
        this.instance = instance;
        this.op = op;
        this.opId = opId;
        this.highestAccepted = highestAccepted;
        this.sn = sn;
    }

    public int getInstance() {
        return instance;
    }

    public UUID getOpId() {
        return opId;
    }

    public byte[] getOp() {
        return op;
    }

    public int getHighestAccepted() {
        return highestAccepted;
    }

    public int getSn() {
        return sn;
    }

    @Override
    public String toString() {
        if(op == null)
            return "PrepareOkMessage{" +
                    "opId=" + opId +
                    ", instance=" + instance +
                    ", op=" + op +
                    ", highestAccepted=" + highestAccepted +
                    ", sn=" + sn +
                    '}';

        return "PrepareOkMessage{" +
                "opId=" + opId +
                ", instance=" + instance +
                ", op=" + Hex.encodeHexString(op) +
                ", highestAccepted=" + highestAccepted +
                ", sn=" + sn +
                '}';
    }

    public static ISerializer<PrepareOkMessage> serializer = new ISerializer<PrepareOkMessage>() {
        @Override
        public void serialize(PrepareOkMessage msg, ByteBuf out) {
            out.writeInt(msg.instance);
            if(msg.opId == null)
                out.writeInt(-1);
            else {
                out.writeInt(1);
                out.writeLong(msg.opId.getMostSignificantBits());
                out.writeLong(msg.opId.getLeastSignificantBits());
                out.writeInt(msg.op.length);
                out.writeBytes(msg.op);
            }
            out.writeInt(msg.highestAccepted);
            out.writeInt(msg.sn);
        }

        @Override
        public PrepareOkMessage deserialize(ByteBuf in) {
            UUID opId = null;
            byte[] op = null;
            int instance = in.readInt();
            if(in.readInt() == 1) {
                long highBytes = in.readLong();
                long lowBytes = in.readLong();
                opId = new UUID(highBytes, lowBytes);
                op = new byte[in.readInt()];
                in.readBytes(op);
            }
            int highestAccepted = in.readInt();
            int sn = in.readInt();
            return new PrepareOkMessage(instance, opId, op, highestAccepted, sn);
        }
    };

}
