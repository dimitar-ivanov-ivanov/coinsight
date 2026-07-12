package coinsight.arbitrage.shared.util;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

/**
 * Per-JVM identity constants, generated once at class-load time.
 *
 * <p>Used by {@code LeaderElectorService} (ingestor's Redis-based leader election) and by
 * {@code coinsight.arbitrage.shared.monitoring.MonitoringMapper} (monitoring events) - both
 * just need a stable way to identify "this running instance", which is all this class provides.
 * It doesn't contain any leader-election or monitoring logic itself.
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
