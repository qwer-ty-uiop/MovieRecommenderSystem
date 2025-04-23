package com.ty.stream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

public class Application {
    public static void main(String[] args) {
        String brokers = "192.168.207.202:9092";
        String from = "log";
        String to = "recommender"; // 输出主题

        StreamsConfig config = getStreamsConfig(brokers);
        StreamsBuilder builder = new StreamsBuilder();

        builder.stream(from, Consumed.with(Serdes.String(), Serdes.String()))
                .filter((key, value) -> value != null && value.contains("MOVIE_RATING_PREFIX:"))
                .mapValues(value -> {
                    String[] parts = value.split("MOVIE_RATING_PREFIX:");
                    return (parts.length > 1) ? parts[1].trim() : null;
                })
                .filter((key, value) -> value != null)
                .to(to, Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();
        System.out.println("Kafka Streams started. Output to: " + to);
    }

    private static StreamsConfig getStreamsConfig(String brokers) {
        Properties settings = new Properties();
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "logFilter");
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        settings.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        settings.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        settings.put(StreamsConfig.STATE_DIR_CONFIG, "data/kafka-streams-state");
        return new StreamsConfig(settings);
    }
}