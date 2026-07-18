package coinsight.arbitrage.aggregations.services;

import coinsight.arbitrage.aggregations.repositories.TickerRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;

@Service
@Slf4j
@RequiredArgsConstructor
public class AggregationsService {

    private final TickerRepository tickerRepository;

    public void getAggregations(LocalDateTime startDate, LocalDateTime endDate, String exchange) {

        // TODO: implement

        // decide which Tier will be used
        //1 hour range - show minute by minute
        //1 day - show 30 minutes by 30 minutes
        //1 week- show 30 minutes by 30 minutes
        //1 month - show hour by hour
        //1 year - show day by day

        if (StringUtils.isBlank(exchange)) {

        }

        // temp statement
        System.out.println(exchange);
    }
}
