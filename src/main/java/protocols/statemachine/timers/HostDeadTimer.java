package protocols.statemachine.timers;
import pt.unl.fct.di.novasys.babel.generic.ProtoTimer;
import pt.unl.fct.di.novasys.network.data.Host;

public class HostDeadTimer extends ProtoTimer {

    public static final short TIMER_ID = 201;

    private Host host;

    public HostDeadTimer(Host host) {
        super(TIMER_ID);
        this.host = host;
    }

    public Host getHost() {
        return host;
    }

    public void setHost(Host host) {
        this.host = host;
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }

}