package coinsight.arbitrage.ingestor.services;

import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Duration;

import static coinsight.arbitrage.ingestor.util.LeaderUtil.INSTANCE_ID;

@Service
public class LeaderElectorService {

    private static final String LEADER_KEY = "ingestor:leader";

    // in-memory flag for checking if the current instance is
    // the leader, if it is the leader it means it can emit events to exchange topics.
    private volatile boolean isLeader = false;

    @Autowired
    private RedisTemplate<String, String> redis;


    public boolean isLeader() {
        return isLeader;
    }

    /**
     * Job that runs every 5 sec and calls Redis.
     * Done, because otherwise we'd have to check Redis per event.
     * If Redis is successfully called the current instance maintains leadership.
     * If the current instance goes down it won't maintain leadership and another
     * instance will take it.
     * The idea of this check is to ensure that scaling horizontally the app won't lead to duplicate events
     * published to the exchange topic as we'll have multiple sockets
     */
    @PostConstruct
    @Scheduled(fixedRate = 5000)
    private void maintainLeadership() {
        redis.opsForValue()
                .setIfAbsent(LEADER_KEY, INSTANCE_ID, Duration.ofSeconds(15));

        String currentLeader = redis.opsForValue().get(LEADER_KEY);
        boolean shouldBeLeader = INSTANCE_ID.equals(currentLeader);

        if (shouldBeLeader && !isLeader) {
            // gain leadership
            isLeader = true;
        } else if (!shouldBeLeader && isLeader) {
            // lose leadership
            isLeader = false;
        }

        // maintain leadership by renewing the lock
        if (isLeader) {
            redis.expire(LEADER_KEY, Duration.ofSeconds(10));
        }
    }
}
