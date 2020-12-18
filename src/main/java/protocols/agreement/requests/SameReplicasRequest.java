package protocols.agreement.requests;

import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;

public class SameReplicasRequest extends ProtoRequest {

    public static final short REQUEST_ID = 404;

    private final int instance;

    public SameReplicasRequest(int instance) {
        super(REQUEST_ID);
        this.instance = instance;
    }

    public int getInstance() {
        return instance;
    }


    @Override
    public String toString() {
        return "SameReplicasRequest{" +
                "instance=" + instance +
                '}';
    }
}