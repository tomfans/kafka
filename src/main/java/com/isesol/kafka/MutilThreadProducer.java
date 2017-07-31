package com.isesol.kafka;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import kafka.javaapi.producer.Producer;

public class MutilThreadProducer implements Runnable  {

	KafkaProducer<String, String> producer = null;
	Properties props = null;
	static ResultScanner resultScanner ;
	
	public MutilThreadProducer() throws IOException{
		System.out.println("start to invoke config");
		props = new Properties();
		props.put("zk.connect",
				"datanode01.isesol.com,datanode02.isesol.com,datanode03.isesol.com,datanode04.isesol.com:2181");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("bootstrap.servers",
				"namenode01.isesol.com:9092,namenode02.isesol.com:9092,datanode03.isesol.com:9092");
		resultScanner = getResult();
	}
	
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
		Result result = null;
		
		System.out.println("start to invoke hbase function");
		synchronized (this) {
			try {
				while (resultScanner.iterator().hasNext()) {
					result = resultScanner.next();
					ProducerRecord<String, String> record = new ProducerRecord<String, String>("jlwang", new String(result.getValue("cf".getBytes(), result.listCells().get(0).getQualifier())));

					System.out.println(Thread.currentThread().getName()  + "  " + new String(result.getRow()));
					try {
						producer.send(record, new Callback() {

							@Override
							public void onCompletion(RecordMetadata metadata, Exception e) {
								if (e != null) {
									e.printStackTrace();
								}
								System.out.println("offset: {} " + metadata.offset() + " partition: {}" + " "
										+ metadata.partition() + " " + metadata.topic());
							}
						});
					} catch (Exception ex) {
						ex.printStackTrace();
					} finally {
						//System.out.println("finish to put data into kafka");
						// producer.close();
					}

				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			//producer.close();
		}
		
		
	}
	
	public static ResultScanner getResult() throws IOException {

		Configuration hbaseconf = HBaseConfiguration.create();
		hbaseconf.set("hbase.zookeeper.quorum",
				"datanode01.isesol.com,datanode02.isesol.com,datanode03.isesol.com,datanode04.isesol.com,cmserver.isesol.com");
		hbaseconf.set("hbase.zookeeper.property.clientPort", "2181");
		hbaseconf.set("user", "hdfs");
		HTable htable = new HTable(hbaseconf, "test11");
		Scan scan = new Scan();
		scan.setCaching(300);
		ResultScanner scaner = htable.getScanner(scan);
		return scaner;

	}
	
	public static void main(String[] args) throws IOException{
		Thread t = new Thread(new MutilThreadProducer()); 
		Thread t2 = new Thread(new MutilThreadProducer()); 
		t.start();	
		t2.start();
	}


}
