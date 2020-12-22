package protocols.agreement.notifications;

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;
import pt.unl.fct.di.novasys.network.data.Host;

import java.util.List;

public class MPJoinedNotification extends ProtoNotification {

    public static final short NOTIFICATION_ID = 404;

    private final List<Host> membership;
    private final int joinInstance;
    private final Host currentLeader;

    public MPJoinedNotification(List<Host> membership, int joinInstance, Host currentLeader) {
        super(NOTIFICATION_ID);
        this.membership = membership;
        this.joinInstance = joinInstance;
        this.currentLeader = currentLeader;
    }

    public int getJoinInstance() {
        return joinInstance;
    }

    public Host getCurrentLeader() {
        return currentLeader;
    }

    public List<Host> getMembership() {
        return membership;
    }

    @Override
    public String toString() {
        return "JoinedNotification{" +
                "membership=" + membership +
                ", joinInstance=" + joinInstance +
                '}';
    }
}
