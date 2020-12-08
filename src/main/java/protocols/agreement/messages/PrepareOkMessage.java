package protocols.agreement.messages;

import io.netty.buffer.ByteBuf;
import org.apache.commons.codec.binary.Hex;
import protocols.app.utils.Operation;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.util.UUID;

public class PrepareOkMessage  extends ProtoMessage {
    public final static short MSG_ID = 402;

    private final UUID opId;
    private final int instance;
    private final byte[] op;
    private final int highestPrepare;

    public PrepareOkMessage(int instance, UUID opId, byte[] op, int highestPrepare) {
        super(MSG_ID);
        this.instance = instance;
        this.op = op;
        this.opId = opId;
        this.highestPrepare = highestPrepare;
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

    public int getHighestPrepare() {
        return highestPrepare;
    }

    @Override
    public String toString() {
        return "PrepareOkMessage{" +
                "opId=" + opId +
                ", instance=" + instance +
                ", op=" + Hex.encodeHexString(op) +
                ", highestPrepare=" + highestPrepare +
                '}';
    }

    public static ISerializer<PrepareOkMessage> serializer = new ISerializer<PrepareOkMessage>() {
        @Override
        public void serialize(PrepareOkMessage msg, ByteBuf out) {
            out.writeInt(msg.instance);
            out.writeLong(msg.opId.getMostSignificantBits());
            out.writeLong(msg.opId.getLeastSignificantBits());
            out.writeInt(msg.op.length);
            out.writeBytes(msg.op);
            out.writeInt(msg.highestPrepare);
        }

        @Override
        public PrepareOkMessage deserialize(ByteBuf in) {
            int instance = in.readInt();
            long highBytes = in.readLong();
            long lowBytes = in.readLong();
            UUID opId = new UUID(highBytes, lowBytes);
            byte[] op = new byte[in.readInt()];
            in.readBytes(op);
            int highestPrepare = in.readInt();
            return new PrepareOkMessage(instance, opId, op, highestPrepare);
        }
    };

}
