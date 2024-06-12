package com.hooniegit.consumer;

import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class ConsumerApplication {

	public static void main(String[] args) {

		// Set Connection Configurations
		String brokers = "220.118.158.230:9092";
		String groupId = "GroupWithTask";
		String topic = "TestTopic";
		int numberOfConsumer = 20;

		// 
		if (args != null && args.length > 4) {
			brokers = args[0];
			groupId = args[1];
			topic = args[2];
			numberOfConsumer = Integer.parseInt(args[3]);
		}

		// Define Group of Notification Consumers
		ConsumerGroup consumerGroup = new ConsumerGroup(brokers, groupId, topic, numberOfConsumer);
		    
		// Start Group Consumer Threads
		consumerGroup.execute();

		try {
			Thread.sleep(100000);
		} catch (InterruptedException ie) {
			// Tasks Here..
		}
	}
}
