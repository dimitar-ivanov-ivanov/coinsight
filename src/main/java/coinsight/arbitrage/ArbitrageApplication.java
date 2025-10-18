package coinsight.arbitrage;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@SuppressWarnings("PMD")
@EnableScheduling
@EnableKafkaStreams
public class ArbitrageApplication {

    public static void main(String[] args) {
        SpringApplication.run(ArbitrageApplication.class, args);
    }

}
