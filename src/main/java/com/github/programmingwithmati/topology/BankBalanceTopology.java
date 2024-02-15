package com.github.programmingwithmati.topology;

import com.github.programmingwithmati.model.BankBalance;
import com.github.programmingwithmati.model.BankTransaction;
import com.github.programmingwithmati.model.JsonSerde;
import org.apache.kafka.common.serialization.Serdes.LongSerde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;

public class BankBalanceTopology {

    public static final String BANK_TRANSACTIONS = "bank-transactions";
    public static final String BANK_BALANCES = "bank-balances";
    public static final String REJECTED_TRANSACTIONS = "rejected-transactions";

    private static final LongSerde LONG_SERDE = new LongSerde();
    private static final JsonSerde<BankBalance> BANK_BALANCE_SERDE = new JsonSerde<>(BankBalance.class);
    private static final JsonSerde<BankTransaction> BANK_TRANSACTION_SERDE = new JsonSerde<>(BankTransaction.class);
    public static final String BANK_BALANCES_STORE = "bank-balances-store";

    public static Topology buildTopology() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        var bankBalanceKStream = streamsBuilder.stream(BANK_TRANSACTIONS, Consumed.with(LONG_SERDE, BANK_TRANSACTION_SERDE))
                .groupByKey()
                .aggregate(BankBalance::new, (key, value, aggregate) -> aggregate.process(value),
                        Materialized.<Long, BankBalance, KeyValueStore<Bytes, byte[]>>as(BANK_BALANCES_STORE)
                                .withKeySerde(LONG_SERDE)
                                .withValueSerde(BANK_BALANCE_SERDE))
                .toStream();

        bankBalanceKStream.to(BANK_BALANCES);

        bankBalanceKStream
                .mapValues(BankBalance::getLatestTransaction)
                .filter((key, value) ->
                        value.getState().equals(BankTransaction.BankTransactionState.REJECTED))
                .to(REJECTED_TRANSACTIONS, Produced.with(LONG_SERDE, BANK_TRANSACTION_SERDE));

        Topology build = streamsBuilder.build();

        System.err.println(build.describe());
        return build;
    }
}
