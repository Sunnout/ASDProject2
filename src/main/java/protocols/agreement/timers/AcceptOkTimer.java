package protocols.agreement.timers;
import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class AcceptOkTimer extends ProtoTimer {

    public static final short TIMER_ID = 400;

    public AcceptOkTimer() {
        super(TIMER_ID);
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }

}