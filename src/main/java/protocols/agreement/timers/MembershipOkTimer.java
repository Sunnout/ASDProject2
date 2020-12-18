package protocols.agreement.timers;
import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class MembershipOkTimer extends ProtoTimer {

    public static final short TIMER_ID = 403;

    private final int instance;

    public MembershipOkTimer(int instance) {
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