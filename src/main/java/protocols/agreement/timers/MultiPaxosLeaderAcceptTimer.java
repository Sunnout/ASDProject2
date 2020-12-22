package protocols.agreement.timers;

import protocols.agreement.requests.ProposeRequest;
import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class MultiPaxosLeaderAcceptTimer extends ProtoTimer{

    public static final short TIMER_ID = 404;

    private ProposeRequest request;

    public MultiPaxosLeaderAcceptTimer(ProposeRequest request) {
        super(TIMER_ID);
        this.request = request;
    }

    public ProposeRequest getRequest() {
        return request;
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }
}
