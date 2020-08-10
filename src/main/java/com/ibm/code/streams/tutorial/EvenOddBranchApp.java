package com.ibm.code.streams.tutorial;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

public class EvenOddBranchApp {

	 public static final String INTEGER_TOPIC_NAME = "integer";
	 public static final String EVEN_TOPIC_NAME = "even";
	 public static final String ODD_TOPIC_NAME = "odd";

	 public static Properties createProperties() {
	     Properties props = new Properties();
	     props.put(StreamsConfig.APPLICATION_ID_CONFIG, "even-odd-branch");
	     props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
	     props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
	     props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
	     return props;
	 }
	 
	 public static Topology createTopology() {
	     final StreamsBuilder builder = new StreamsBuilder();
	     
	     KStream<Integer, String> streamline = builder.stream(INTEGER_TOPIC_NAME);
	     //@SuppressWarnings("unchecked")
	    
		KStream<Integer, String>[] branches = streamline
	             .branch(
	                     (key, value) -> key % 2 == 0,
	                     (key, value) -> true
	             );
	     branches[0]
	             .peek((key, value) -> System.out.printf("even: %s, %s%n", key, value))
	             .mapValues(v -> v.toUpperCase())
	             .to(EVEN_TOPIC_NAME);
	     branches[1]
	             .peek((key, value) -> System.out.printf("odd: %s, %s%n", key, value))
	             .mapValues(v -> v.toLowerCase())
	             .to(ODD_TOPIC_NAME);

	     return builder.build();
	 }
	 public static void main(String[] args) {
	     Properties props = createProperties();

	     final Topology topology = createTopology();
	     final KafkaStreams streams = new KafkaStreams(topology, props);
	     final CountDownLatch latch = new CountDownLatch(1);

	     Runtime.getRuntime().addShutdownHook(new Thread("kafka-streams-shutdown-hook") {
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

}
