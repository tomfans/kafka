package com.isesol.kafka;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

public class kafkaConsumer {
	
	public static void main(String[] args){
		
	     Properties props = new Properties();
	     props.put("bootstrap.servers", "namenode01.isesol.com:9092,namenode02.isesol.com:9092,datanode03.isesol.com:9092");
	     props.put("group.id", "test4");
	     props.put("enable.auto.commit", "true");
	     props.put("auto.commit.interval.ms", "1000");
	     props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	     props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	     props.put("auto.offset.reset", "earliest");
	     KafkaConsumer<String, String> consumer = new KafkaConsumer<String,String>(props);
	    // TopicPartition partition0 = new TopicPartition("jlwang", 1);
	     //TopicPartition partition1 = new TopicPartition("jlwang", 0);
	    // consumer.assign(Arrays.asList(partition1));
	    // consumer.seek(partition1, 491);
	   //  consumer.seek(partition0, 495);
	     consumer.subscribe(Arrays.asList("jlwang"));
	     while (true) {
	         ConsumerRecords<String, String> records = consumer.poll(100);
	         for (ConsumerRecord<String, String> record : records)
	             System.out.printf("partition=%d, offset = %d, key = %s, value = %s%n", record.partition(),record.offset(), record.key(), record.value());
	     }
	 
	}

}
