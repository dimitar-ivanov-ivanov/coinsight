package coinsight.arbitrage.shared.model;

public enum Exchange {
    BINANCE("binance"),
    COINBASE("coinbase");

    private final String value;

    Exchange(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
