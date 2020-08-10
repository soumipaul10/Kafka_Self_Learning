package com.ibm.code.streams.tutorial;


import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;

import org.apache.kafka.clients.producer.Producer;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;

import org.apache.kafka.common.serialization.StringSerializer;


public class IntegerProducer {

	 public static void main(String[] args) {
	     Properties props = new Properties();
	     props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
	     props.put(ProducerConfig.ACKS_CONFIG, "all");
	     props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
	     props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

	     Producer<Integer, String> producer = new KafkaProducer<>(props);
	     for (int i = 0; i < 10; i++) {
	         producer.send(new ProducerRecord<Integer, String>(EvenOddBranchApp.INTEGER_TOPIC_NAME,
	                 new Integer(i), "Value - " + i));
	     }
	     producer.close();
	 }


}
