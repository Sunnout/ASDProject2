package protocols.agreement.timers;
import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;

public class LeaderDeadTimer extends ProtoTimer {

    public static final short TIMER_ID = 205;


    public LeaderDeadTimer() {
        super(TIMER_ID);
    }


    @Override
    public ProtoTimer clone() {
        return this;
    }

}