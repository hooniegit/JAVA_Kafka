package com.hooniegit.consumer;

import java.util.ArrayList;
import java.util.List;

public final class ConsumerGroup {

	private final int numberOfConsumers;
	private final String groupId;
	private final String topic;
	private final String brokers;
	private List<ConsumerThread> consumers;

	// Initialize
	public ConsumerGroup(String brokers, String groupId, String topic, int numberOfConsumers) {
	    this.brokers = brokers;
	    this.topic = topic;
	    this.groupId = groupId;
	    this.numberOfConsumers = numberOfConsumers;
	    
	    // Create Consumer Group
	    consumers = new ArrayList<>();
	    for (int i = 0; i < this.numberOfConsumers; i++) {
	    	ConsumerThread ncThread =
	    		new ConsumerThread(this.brokers, this.groupId, this.topic);
	    	consumers.add(ncThread);
	    }
	}

	// Start Notification Consumer Thread
	public void execute() {
		for (ConsumerThread ncThread : consumers) {
			Thread t = new Thread(ncThread);
			t.start();
		}
	}

	// Return Consumer Count
	public int getNumberOfConsumers() {
		return numberOfConsumers;
	}

	// Return Group Id
	public String getGroupId() {
		return groupId;
	}

}