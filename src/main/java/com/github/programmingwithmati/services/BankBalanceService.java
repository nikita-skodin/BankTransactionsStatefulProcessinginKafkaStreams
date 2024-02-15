package com.github.programmingwithmati.services;

import com.github.programmingwithmati.model.BankBalance;
import com.github.programmingwithmati.topology.BankBalanceTopology;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class BankBalanceService {

    private final KafkaStreams kafkaStreams;

    public BankBalance getBankBalanceById(Long id) {
        return getStore().get(id);
    }

    private ReadOnlyKeyValueStore<Long, BankBalance> getStore() {
        return kafkaStreams.store(StoreQueryParameters.fromNameAndType(
                BankBalanceTopology.BANK_BALANCES_STORE,
                QueryableStoreTypes.keyValueStore()
        ));
    }

}
