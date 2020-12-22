package protocols.agreement.utils;

import pt.unl.fct.di.novasys.network.data.Host;

public class HostAndSn {

    private Host host;
    private int sn;

    public HostAndSn(Host host, int sn) {
        this.host = host;
        this.sn = sn;
    }

    public Host getHost() {
        return host;
    }

    public int getSn() {
        return sn;
    }
}
