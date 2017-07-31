package com.isesol.kafka;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.*;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import scala.util.control.Exception.Catch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.producer.*;
import org.jboss.netty.channel.ReceiveBufferSizePredictor;

public class kafkaProducer {

	public static void main(String[] args) throws IOException {

		Properties props = new Properties();
		props.put("zk.connect",
				"datanode01.isesol.com,datanode02.isesol.com,datanode03.isesol.com,datanode04.isesol.com:2181");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("bootstrap.servers",
				"namenode01.isesol.com:9092,namenode02.isesol.com:9092,datanode03.isesol.com:9092");
		//props.put("batch.size", "3200000");
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
		Result result = null;
		
		while (getResult().iterator().hasNext()) {
			StringBuilder builder = new StringBuilder();
			result = getResult().next();

			for (int i = 0; i < result.size() - 1; i++) {
				builder.append(
						new String(result.getValue("cf".getBytes(), result.listCells().get(i).getQualifier())) + "#");
			//	System.out.println(
			//			new String(result.getValue("cf".getBytes(), result.listCells().get(i).getQualifier())));
			}

		//	System.out.println("the data is " + builder.toString());
			ProducerRecord<String, String> record = new ProducerRecord<String, String>("jlwang", builder.toString());

			try {
				producer.send(record, new Callback() {

					@Override
					public void onCompletion(RecordMetadata metadata, Exception e) {
						if (e != null) {
							e.printStackTrace();
						}
				//		System.out.println("offset: {} " + metadata.offset() + " partition: {}" + " "
				//				+ metadata.partition() + " " + metadata.topic());
					}
				});
			} catch (Exception ex) {
				ex.printStackTrace();
			} finally {
				//System.out.println("finish to put data into kafka");
				// producer.close();
			}

		}

		producer.close();
	}

	public static ResultScanner getResult() throws IOException {

		Configuration hbaseconf = HBaseConfiguration.create();
		hbaseconf.set("hbase.zookeeper.quorum",
				"datanode01.isesol.com,datanode02.isesol.com,datanode03.isesol.com,datanode04.isesol.com,cmserver.isesol.com");
		hbaseconf.set("hbase.zookeeper.property.clientPort", "2181");
		hbaseconf.set("user", "hdfs");
		HTable htable = new HTable(hbaseconf, "t_kafka_topic_2001");
		Scan scan = new Scan();
		scan.setCaching(300);
		ResultScanner scaner = htable.getScanner(scan);
		return scaner;

	}
}
