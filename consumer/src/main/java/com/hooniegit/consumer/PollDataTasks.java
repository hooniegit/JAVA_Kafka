package com.hooniegit.consumer;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class PollDataTasks {
	// ** Need to Re-Define Types **
	// Use ConsumerRecords<String, String> records

    void run(ConsumerRecord<String, String> record) {
        System.out.println("[Consumer] Task Threads Started"); // Log

        // Create Tasks List
        List<Thread> threadList = new ArrayList<>();
        // ** Add Functions Here.. **
        threadList.add(new Thread(() -> {
			try {
				SAMPLE_FileWriter(record);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}));
        threadList.add(new Thread(() -> SAMPLE_Hello()));

        // Run Threads
        for (Thread thread : threadList) {
        		thread.start();
        }

        // Join Threads
        for (Thread thread : threadList) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println("[Consumer] Task Threads Finished"); // Log
    }
	
	// SAMPLE - Write Polled Data as a Log File
	public static void SAMPLE_FileWriter(ConsumerRecord<String, String> record) throws IOException{
		// Reference Path
		String logDir = "C:\\Users\\dhkim\\Desktop\\Logs\\KafkaJavaLogs\\";

		// Dispatch Result Data
        String topic = record.topic();
		int partition = record.partition();
		String key = record.key();
		String value = record.value(); // ** Need to Re-Define Types **
		long timestamp = record.timestamp();
		
		// Design
		String filePath = logDir + String.valueOf(partition) + ".log";
		String content = String.format("[INFO] value: %s, timestamp: %d\n", value, timestamp); 
		
        // Write to file (Avoid IO Exception While Running on Stream)
        BufferedWriter writer = null;
        try {
            writer = new BufferedWriter(new FileWriter(filePath, true));
            writer.write(content);
        } finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
	}
        
	// SAMPLE - Say Hello to Kafka
	public static void SAMPLE_Hello() {
		System.out.println("Hello, Kafka!");
	}
}
