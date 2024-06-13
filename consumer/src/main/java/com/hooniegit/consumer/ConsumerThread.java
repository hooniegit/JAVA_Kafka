package com.hooniegit.consumer;

import java.util.Map;
import java.util.HashMap;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.Collection;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

//Implements Runnable (to Run run() in Thread)
public class ConsumerThread implements Runnable {
	private final KafkaConsumer<String, String> consumer;
	private final String topic;
	public PollDataTasks pollDataTasks;
	private Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
	Duration timeout = Duration.ofMillis(100);


	// Initialize
	public ConsumerThread(String brokers, String groupId, String topic) {
		// Create Configuration
		Properties prop = createConsumerConfig(brokers, groupId);
		
		// Create Consumer (with Configurations)
		this.consumer = new KafkaConsumer<>(prop);
		
		// Subscribe Topic
		this.topic = topic;
		this.consumer.subscribe(Arrays.asList(this.topic), new HandleRebalance());
		
		// Initialize Tasks;
		this.pollDataTasks = new PollDataTasks();
	}
	
	// Re-Balance Handler
	private class HandleRebalance implements ConsumerRebalanceListener{
		// Tasks When Re-Balancing Happens : Get Partitions
		public void onPartitionsAssigned(Collection<TopicPartition>partitions) {
			// Add Tasks Here..
		}
		
		// Tasks When Re-Balancing Happens : Lost Partitions
		public void onPartitionsRevoked(Collection<TopicPartition>partitions) {
			// Add Tasks Here..
			consumer.commitSync(currentOffsets);
		}
	}
	
	// Set Consumer Properties Configuration
	// ** Need to Change Several Parameters Include Auto Commit **
	private static Properties createConsumerConfig(String brokers, String groupId) {
		// Basic Properties
		Properties props = new Properties();
		props.put("bootstrap.servers", brokers);
		props.put("group.id", groupId);
		props.put("enable.auto.commit", "False"); // Do Not Commit Offsets Automatically (Can't Handle Issues)
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("auto.offset.reset", "earliest");
		// ** Need to Use External De-Serializer **
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		// Cooperative Re-Balance & Sticky Assign
		props.put("partition.assignment.strategy", "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");
		return props;
	}

	// Run Single Consumer
	// Add Override Option to Run in Thread
	@Override
	public void run() {
		try {
			while (true) {
				// Poll Data from Server
				// ** Need to Re-Define Types **
				ConsumerRecords<String, String> records = consumer.poll(timeout);
				
				// Do Tasks Based on Polled Data & Commit Offset
				for (ConsumerRecord<String, String> record : records) {
					// Run Tasks
					pollDataTasks.run(record);
					// SAMPLE LOG
					System.out.println("Receive message: " + record.value() + ", Partition: "
						+ record.partition() + ", Offset: " + record.offset() + ", by ThreadID: "
						+ Thread.currentThread().getId());
				} consumer.commitAsync(currentOffsets, null);
	
			}
			
		} catch (WakeupException e) {
			// Add Logging Logics Here..
		} catch (Exception e) {
			// Add Logging Logics Here..
		} finally {
			try {
				// Commit Offset After Re-Balancing
				consumer.commitSync(currentOffsets);
			} finally {

				consumer.close();
				// Add Logging Logics Here..
			}
		}
	}
	
	// Wake Up Consumer (when Shut Down Happens)
    public void shutdown() {
        consumer.wakeup();
    }
}