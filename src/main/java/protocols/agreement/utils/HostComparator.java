package protocols.agreement.utils;

import pt.unl.fct.di.novasys.network.data.Host;

import java.util.Comparator;

public class HostComparator implements Comparator<Host> {

    @Override
    public int compare(Host h1, Host h2) {

        if (h1.getPort() < h2.getPort())
            return -1;
        else if (h1.getPort() > h2.getPort())
            return 1;
        return h1.getAddress().toString().compareTo(h2.getAddress().toString());
    }
}

