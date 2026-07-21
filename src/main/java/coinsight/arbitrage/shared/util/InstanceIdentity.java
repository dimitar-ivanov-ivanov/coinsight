package coinsight.arbitrage.shared.util;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

/**
 * Per-JVM identity constants, generated once at class-load time.
 */
public class InstanceIdentity {

    public static final String INSTANCE_ID = UUID.randomUUID().toString();

    public static final String HOSTNAME;

    static {
        try {
            HOSTNAME = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
    }
}
