package protocols.agreement.timers;
import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class PrepareOkTimer extends ProtoTimer {

    public static final short TIMER_ID = 400;

    public PrepareOkTimer() {
        super(TIMER_ID);
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }

}