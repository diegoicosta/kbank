package kbank.account;

import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AccountInvariances {

    private Logger log = LoggerFactory.getLogger(this.getClass());
    private final ReadOnlyKeyValueStore<String, Long> balanceByAccountStore;

    public AccountInvariances(final ReadOnlyKeyValueStore<String, Long> balanceByAccountStore) {
        this.balanceByAccountStore = balanceByAccountStore;
    }

    public boolean hasBalance(String account, long value) {
        Long balance = balanceByAccountStore.get(account);
        balance = balance == null ? 0 : balance;
        log.info("Checking account {} Has Balance ? {}", account, balance + value >= 0);
        return balance + value >= 0;
    }

    public boolean noBalance(String account, long value) {
        Long balance = balanceByAccountStore.get(account);
        balance = balance == null ? 0 : balance;
        log.info("Checking account {} Has NO Balance ? {}", account, balance + value < 0);
        return balance + value < 0;
    }

}
