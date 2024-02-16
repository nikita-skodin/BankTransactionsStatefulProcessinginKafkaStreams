package com.github.programmingwithmati.topology;

import com.github.programmingwithmati.model.BankBalance;
import com.github.programmingwithmati.model.BankTransaction;
import com.github.programmingwithmati.model.JsonSerde;
import org.apache.kafka.common.serialization.Serdes.LongSerde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;

//@Log4j2
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

        var bankBalanceKStream = streamsBuilder.stream(BANK_TRANSACTIONS,

                        Consumed.with(LONG_SERDE, BANK_TRANSACTION_SERDE)
                                .withTimestampExtractor((record, partitionTime) ->
                                        ((BankTransaction) record.value()).getTime().toInstant().getEpochSecond()))

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

        streamsBuilder.stream(REJECTED_TRANSACTIONS, Consumed.with(LONG_SERDE, BANK_TRANSACTION_SERDE))
                .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofSeconds(20)).grace(Duration.ofSeconds(2)))
                .count()
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
                .toStream()
                .peek((key, value) -> System.out.printf("Peek: %s:%s, key: %s%n", key.key(), value, key))
                .map((key, value) -> KeyValue.pair(key.key(), value))
                .filter((key, value) -> value >= 10)
//                .mapValues()
                .to("possible-fraud-topic", Produced.with(LONG_SERDE, LONG_SERDE));


        Topology build = streamsBuilder.build();
        System.err.println(build.describe());
        return build;
    }
}