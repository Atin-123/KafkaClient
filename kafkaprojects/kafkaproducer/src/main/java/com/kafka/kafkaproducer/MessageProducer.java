package com.kafka.kafkaproducer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//Steps to create a synchronous Kafka Producer.
//first create KafkaProducer reference variable with proper datatype of Key and value.
//next create a map to put the configs. Here I'm using hashmap.
//

public class MessageProducer {

	private static final Logger logger = LoggerFactory.getLogger(MessageProducer.class);

	String topicName = "test-topic-replicated";
	KafkaProducer<String, String> kafkaProducer;

	public MessageProducer(Map<String, Object> propsMap) {
		kafkaProducer = new KafkaProducer<String, String>(propsMap);
	}

	Callback callback = (metadata, exception) -> {
		if (exception != null) {
			logger.error("Exception Occured. Exception is-", exception.getMessage());
		} else {
			logger.info("Message published successfully. partition is = {}", metadata.partition());
		}
	};

	public void sendMessageAsync(String key, String value) {
		ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicName, key, value);
		kafkaProducer.send(record, callback);
	}

	public void sendMessageSync(String key, String value) {
		ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topicName, key, value);

		try {
			RecordMetadata metadata = kafkaProducer.send(producerRecord).get();
//			System.out.println("Partition is: "+metadata.partition()+"\nand\n "+"Offset is: "+metadata.offset());
			logger.info("The key sent is: {} and the value sent is {}", key, value);
			logger.info("The Partition is: {} and the offset is {}", metadata.partition(), metadata.offset());
		} catch (InterruptedException e) {
//			e.printStackTrace();
			logger.error(e.getMessage());
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws InterruptedException {

		Map<String, Object> propsMap = new HashMap<String, Object>();
		propsMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092, localhost:9094, localhost:9096");
		propsMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		propsMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		propsMap.put(ProducerConfig.ACKS_CONFIG, "all");
		
		MessageProducer messageProducer = new MessageProducer(propsMap);
		messageProducer.sendMessageSync(null, "DEF");
//		messageProducer.sendMessageAsync(null, "ABD-Async");
		Thread.sleep(3000);

	}

}
