package coinsight.arbitrage.aggregations.controllers;

import coinsight.arbitrage.aggregations.services.AggregationsService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;

@RestController
@RequestMapping("/api/v1/aggregations")
@RequiredArgsConstructor
public class AggregationsController {

    private final AggregationsService aggregationsService;

    @GetMapping("/{startData}/{endDate}")
    public void readAggregation(
            @PathVariable("startDate") LocalDateTime startDate,
            @PathVariable("endDate") LocalDateTime endDate,
            @RequestParam(value = "exchange", required = false) String exchange) {

        aggregationsService.getAggregations(startDate, endDate, exchange);

    }

}
