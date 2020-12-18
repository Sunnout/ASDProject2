package protocols.agreement.timers;
import protocols.agreement.requests.ProposeRequest;
import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class MultiPaxosStartTimer extends ProtoTimer {

    public static final short TIMER_ID = 403;

    private ProposeRequest request;

    public MultiPaxosStartTimer(ProposeRequest request) {
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