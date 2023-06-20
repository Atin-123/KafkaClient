package com.kafka.kafkaconsumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class MessageConsumer {
	KafkaConsumer<String, String> kafkaConsumer;
	String topicName = "test-topic-replicated";
	
	private static final Logger logger = LoggerFactory.getLogger(MessageConsumer.class);
	
	public MessageConsumer(Map<String, Object> propsMap) {
		kafkaConsumer = new KafkaConsumer<String, String>(propsMap);
	}
	
	public void fetchMessage() {
		List<String> topicList = Arrays.asList(topicName);
		kafkaConsumer.subscribe(topicList);
		
		Duration timeout = Duration.of(100, ChronoUnit.MILLIS);
		
		try {
			while(true){
				ConsumerRecords<String, String>records = kafkaConsumer.poll(timeout);
				Iterator<ConsumerRecord<String, String>> recordIterator = records.iterator();
				while(recordIterator.hasNext()) {
					ConsumerRecord<String, String>record = recordIterator.next();
					logger.info("The key of the message is - {} and the value of the message is - {} and the Partition of the message is - {}", record.key(), record.value(), record.partition());
				}
			}
		}catch(Exception e) {
			logger.error("Exception Occured. Exception is- {}", e.getMessage());
		}finally {
			kafkaConsumer.close();
		}
		
		
	}
	
	public static void main(String[] args) {
		Map<String, Object> propsMap = new HashMap<String, Object>();
		propsMap.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092, localhost:9094, localhost:9096");
		propsMap.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		propsMap.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		propsMap.put(ConsumerConfig.GROUP_ID_CONFIG, "messageconfig");
		
		MessageConsumer consumer = new MessageConsumer(propsMap);
		consumer.fetchMessage();
	}

}
