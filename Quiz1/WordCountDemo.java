
package org.apache.kafka.streams.examples.wordcount;
// package org.apache.kafka.streams.examples.temperature;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.streams.kstream.Suppressed;

public final class WordCountDemo {

    public static final String INPUT_TOPIC = "streams-plaintext-input";
    public static final String OUTPUT_TOPIC = "streams-wordcount-output";

	// window size within which the filtering is applied
    private static final int TEMPERATURE_WINDOW_SIZE = 5;

    static Properties getStreamsConfig(final String[] args) throws IOException {
		
		final Properties props = new Properties();
		if (args != null && args.length > 0) {
			try (final FileInputStream fis = new FileInputStream(args[0])) {
				props.load(fis);
			}
			if (args.length > 1) {
				System.out.println("Warning: Some command line arguments were ignored. This demo only accepts an optional configuration file.");
			}
		}
        
		props.putIfAbsent(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount");
		props.putIfAbsent(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.putIfAbsent(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
		props.putIfAbsent(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.putIfAbsent(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

		// setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
		// Note: To re-run the demo, you need to use the offset reset tool:
		// https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Streams+Application+Reset+Tool
		props.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		return props;
	}

    static void createWordCountStream(final StreamsBuilder builder) {
        final KStream<String, String> source = builder.stream(INPUT_TOPIC);

		// Harry Catching
		final KTable<Windowed<String>, Long> catch_harry = source
			// .flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split(" ")))
            .flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+")))
			// .filter((key, value) -> value.equals("harry")) // .contain or equals()
            // .groupBy((key, value) -> value)
			// .windowedBy(TimeWindows.of(Duration.ofSeconds(TEMPERATURE_WINDOW_SIZE)))
            // .count()
			// .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()));
			.filter((key, value) -> value.equals("harry"))
			.groupBy((key, value) -> value)
			.windowedBy(TimeWindows.of(Duration.ofSeconds(TEMPERATURE_WINDOW_SIZE)))
			.count();
			// .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
			
			// .toStream();


		// ---------------------------------------------------------------------------------------------------
        // temperature values are sent without a key (null), so in order
        // to group and reduce them, a key is needed ("temp" has been chosen)
        // https://kafka.apache.org/23/javadoc/org/apache/kafka/streams/kstream/KStream.html
        // final KStream<Windowed<String>, Long> max = catch_harry
        //     .selectKey((key, value) -> key)
        //     .groupBy()
        //     .windowedBy(TimeWindows.of(Duration.ofSeconds(5)))
        //     .reduce((value1, value2) -> {
        //         if (Integer.parseInt(value1) > Integer.parseInt(value2)) {
        //             return value1;
        //         } else {
        //             return value2;
        //         }
        //     })
        //     .toStream();
        //     //.filter((key, value) -> Integer.parseInt(value) > TEMPERATURE_THRESHOLD);

        // final Serde<Windowed<String>> windowedSerde = WindowedSerdes.timeWindowedSerdeFrom(String.class);

        // // need to override key serde to Windowed<String> type
        // max.to(OUTPUT_TOPIC, Produced.with(windowedSerde, Serdes.String()));
		// ---------------------------------------------------------------------------------------------------

		// Go ahead Harry
		// catch_harry.toStream().to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));

		// Go ahead Harry 2
		final Serde<Windowed<String>> windowedSerde = WindowedSerdes.timeWindowedSerdeFrom(String.class);
		catch_harry.toStream().to(OUTPUT_TOPIC, Produced.with(windowedSerde, Serdes.Long()));


    }

    public static void main(final String[] args) {
        //final Properties props = getStreamsConfig();
		
		try{
			
			final Properties props = getStreamsConfig(args);
			final StreamsBuilder builder = new StreamsBuilder();
			createWordCountStream(builder);
			final KafkaStreams streams = new KafkaStreams(builder.build(), props);
			final CountDownLatch latch = new CountDownLatch(1);

			// attach shutdown handler to catch control-c
			Runtime.getRuntime().addShutdownHook(new Thread("streams-wordcount-shutdown-hook") {
				@Override
				public void run() {
					streams.close();
					latch.countDown();
				}
			});
            streams.start();
            latch.await();
		
		}
		catch(IOException e) {
			e.printStackTrace();
        } catch (final Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
