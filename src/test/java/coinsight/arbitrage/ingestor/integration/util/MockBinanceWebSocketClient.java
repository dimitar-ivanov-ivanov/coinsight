package coinsight.arbitrage.ingestor.integration.util;

import coinsight.arbitrage.ingestor.components.BinanceWebSocketClient;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Profile("test")
public class MockBinanceWebSocketClient extends BinanceWebSocketClient {

    /**
     * Mock for BinanceWebSocketClient.
     * Made so that the real stream is not called
     *
     * @throws Exception
     */
    public MockBinanceWebSocketClient() throws Exception {
    }
}
