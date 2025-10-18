package coinsight.arbitrage.ingestor.util;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

/**
 * Util class for instance and host.
 * Both are used for monitoring events.
 * The INSTANCE_ID is used for leader election for ingestor.
 */
public class LeaderUtil {

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
