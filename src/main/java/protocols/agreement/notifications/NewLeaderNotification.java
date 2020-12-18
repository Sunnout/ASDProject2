package protocols.agreement.notifications;

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;
import pt.unl.fct.di.novasys.network.data.Host;

public class NewLeaderNotification extends ProtoNotification {

    public static final short NOTIFICATION_ID = 403;

    private final Host newLeader;

    public NewLeaderNotification(Host newLeader) {
        super(NOTIFICATION_ID);
        this.newLeader = newLeader;
    }

    public Host getNewLeader() {
        return newLeader;
    }

    @Override
    public String toString() {
        return "NewLeaderNotification{" +
                "leader=" + newLeader +
                '}';
    }
}