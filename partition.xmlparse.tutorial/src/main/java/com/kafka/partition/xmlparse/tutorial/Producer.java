package com.kafka.partition.xmlparse.tutorial;

import org.apache.kafka.clients.producer.*;
import org.w3c.dom.Document;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathFactory;


public class Producer {
	//private static Scanner in;

	public static void main(String[] argv) throws Exception {
		if (argv.length != 1) {
			System.err.println("Please specify 1 parameters ");
			System.exit(-1);
		}
		String topicName = argv[0];
		File dir = new File("C:\\Users\\AngeL\\Kafka\\streams.tutorial\\src\\main\\java\\CustomerXml");
		File[] directoryListing = dir.listFiles();
		DocumentBuilderFactory builderFactory =
		        DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = null;
		int counter=0;
		try {
		    builder = builderFactory.newDocumentBuilder();
		} catch (ParserConfigurationException e) {
		    e.printStackTrace();  
		}
		
	
		Properties configProperties = createProperties();
		org.apache.kafka.clients.producer.Producer producer = new KafkaProducer(configProperties);
		int count=dir.list().length;
		
		while (counter!=dir.list().length) {
	           
			String xml = ""; 
			String xmlname = directoryListing[counter].toString();
			xml = new String(Files.readAllBytes(Paths.get(xmlname)));
			Document xmlDocument = builder.parse(new ByteArrayInputStream(xml.getBytes()));
			XPath xPath =  XPathFactory.newInstance().newXPath();
			String expression= "/SynchroniseCustomer[@releaseID=\"1.0\"]/DataArea/Customer/BrandChannel/Brand/Code[@name=\"B and Q\"]/text()";
			String countryKey = xPath.compile(expression).evaluate(xmlDocument);
			ProducerRecord<String,String> rec = new ProducerRecord<String,String>
												(topicName,countryKey+":"+xml);
			producer.send(rec, new Callback() {
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					System.out.println("Message sent to topic ->" + metadata.topic() + " ," + "parition->"
							+ metadata.partition() + " stored at offset->" + metadata.offset());
				}
			});
			counter=counter+1;
		}
		//in.close();
		producer.close();
	}

	private static Properties createProperties() {
		// Configure the Producer
		Properties configProperties = new Properties();
		configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringSerializer");
		configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringSerializer");

		configProperties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CountryPartitioner.class.getCanonicalName());
		configProperties.put("partitions.0", "BQ");
		configProperties.put("partitions.1", "CAFR");
		configProperties.put("partitions.2", "BDFR");

		return configProperties;
	}
}
