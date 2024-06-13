package com.hooniegit.consumer;

import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

// Implements Runnable (to Run run() in Thread)
public class ExecutorManager implements Runnable {
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();
    private final ConsumerThread consumerRunnable;

    // Initialize
    public ExecutorManager(ConsumerThread consumerRunnable) {
        this.consumerRunnable = consumerRunnable;
    }
    
    // Add Override Option to Run in Thread
    @Override
    public void run() {
    	start();
    }

    // Start Single Consumer Thread with Managing Options
    public void start() {
        executorService.submit(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                	// Run Consumer
                    consumerRunnable.run();
                } catch (Exception e) {
                    e.printStackTrace(); // Log Exception
  
                    try {
                    	// Wait Before Restarting
                    	// ** Need to Change Time Range.. **
                        TimeUnit.SECONDS.sleep(5);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        });
    }

    // Shut Down Consumer Thread & Manager
    public void shutdown() {
        consumerRunnable.shutdown();
        executorService.shutdown();
        try {
        	// ** Need to Change Time Range.. **
            if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
        }
    }
}
