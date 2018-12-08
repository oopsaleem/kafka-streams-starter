package com.gwb.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class WordCountApp {
    private static Logger log = LoggerFactory.getLogger(WordCountApp.class);

    public static void main(String[] args) {
        Properties props = new Properties();
        //StreamConfig
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-starter");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.214.128:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        //ConsumerConfig
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> source = builder.stream("streams-plaintext-input");
        source.flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+")))
                .groupBy((key, value) -> value)
                .count(Materialized.as("counts-store"))
                .toStream()
                .to("streams-wordcount-output", Produced.with(Serdes.String(), Serdes.Long()));

        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        printLogo();

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

    public static void printLogo() {
        final String RED_COLOR = "\u001b["  // Prefix - see [1]
                + "0"        // Brightness
                + ";"        // Separator
                + "31"       // Red foreground
                + "m"        // Suffix
        ;
        final String RESET_COLOR = "\u001b[m ";
//        for (int i = 0; i < 90; i++) {
//            log.info("\u001B[0;"+i+"mNice COLOR="+i+RESET_COLOR);
//        }
        log.info("\u001B[0;36m" + "\n" +
                " ____   __   ____  _  _    __ _   __    ___ \n" +
                "(  __) / _\\ / ___)( \\/ )  (  ( \\ /  \\  / __)\n" +
                " ) _) /    \\\\___ \\ )  /   /    /(  O )( (__ \n" +
                "(____)\\_/\\_/(____/(__/    \\_)__) \\__/  \\___)\n" +
                RESET_COLOR
        );
//        log.info(RED_COLOR +
//                "\n" +
//                "   ____                          _  __  ____   _____\n" +
//                "  / __/ ___ _  ___  __ __       / |/ / / __ \\ / ___/\n" +
//                " / _/  / _ `/ (_-< / // /      /    / / /_/ // /__  \n" +
//                "/___/  \\_,_/ /___/ \\_, /      /_/|_/  \\____/ \\___/  \n" +
//                "                  /___/                             \n"
//                + RESET_COLOR); // Prefix + Suffix to reset color
    }
}
