package protocols.agreement.messages;

import io.netty.buffer.ByteBuf;
import org.apache.commons.codec.binary.Hex;
import protocols.app.utils.Operation;
import protocols.statemachine.utils.OperationAndId;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.ISerializer;

import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

public class MPPrepareOkMessage  extends ProtoMessage {
    public final static short MSG_ID = 406;

    private final int instance;
    private final int sn;
    private List<OperationAndId> ops;


    public MPPrepareOkMessage(int instance, List<OperationAndId> ops, int sn) {
        super(MSG_ID);
        this.instance = instance;
        this.ops = ops;
        this.sn = sn;
    }

    public int getInstance() {
        return instance;
    }

    public List<OperationAndId> getOps() {
        return ops;
    }

    public int getSn() {
        return sn;
    }

    @Override
    public String toString() {
        return "MPPrepareOkMessage{" +
                "ops=" + ops +
                ", instance=" + instance +
                ", sn=" + sn +
                '}';
    }

    public static ISerializer<MPPrepareOkMessage> serializer = new ISerializer<MPPrepareOkMessage>() {
        @Override
        public void serialize(MPPrepareOkMessage msg, ByteBuf out) {
            try {
                out.writeInt(msg.instance);
                out.writeInt(msg.sn);
                out.writeInt(msg.ops.size());
                for (OperationAndId op : msg.ops) {
                    out.writeLong(op.getOpId().getMostSignificantBits());
                    out.writeLong(op.getOpId().getLeastSignificantBits());
                    out.writeInt(op.getOperation().toByteArray().length);
                    out.writeBytes(op.getOperation().toByteArray());
                }
            }catch (Exception e){
                e.printStackTrace();
            }
        }

        @Override
        public MPPrepareOkMessage deserialize(ByteBuf in) {
            try {
                UUID opId = null;
                byte[] op = null;
                List<OperationAndId> ops = new LinkedList<>();
                int instance = in.readInt();
                int sn = in.readInt();
                int aux = in.readInt();
                for (int i = 0; i < aux; i++) {
                    long highBytes = in.readLong();
                    long lowBytes = in.readLong();
                    opId = new UUID(highBytes, lowBytes);
                    op = new byte[in.readInt()];
                    in.readBytes(op);
                    ops.add(new OperationAndId(Operation.fromByteArray(op), opId));
                }
                return new MPPrepareOkMessage(instance, ops, sn);

            }
            catch (Exception e){
                e.printStackTrace();
            }
            return  null;
        }
    };

}
