package protocols.agreement.timers;
import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class AcceptOkTimer extends ProtoTimer {

    public static final short TIMER_ID = 401;
    private final int instance;

    public AcceptOkTimer(int instance) {
        super(TIMER_ID);
        this.instance = instance;
    }

    public int getInstance() {
        return instance;
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }

}