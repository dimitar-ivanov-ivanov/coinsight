package coinsight.arbitrage.aggregations.controllers;

import coinsight.arbitrage.aggregations.pojo.OhlcPoint;
import coinsight.arbitrage.aggregations.services.AggregationsService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.OffsetDateTime;
import java.util.List;

@RestController
@RequestMapping("/api/v1/aggregations")
@RequiredArgsConstructor
public class AggregationsController {

    private final AggregationsService aggregationsService;

    @GetMapping("/{cryptoPair}/{startDate}/{endDate}")
    public ResponseEntity<List<OhlcPoint>> readAggregation(
            @PathVariable("cryptoPair") String cryptoPair,
            @PathVariable("startDate") OffsetDateTime startDate,
            @PathVariable("endDate") OffsetDateTime endDate,
            @RequestParam(value = "exchange", required = false) String exchange) {

        return ResponseEntity.ok(aggregationsService.getAggregations(cryptoPair, startDate, endDate, exchange));
    }
}
