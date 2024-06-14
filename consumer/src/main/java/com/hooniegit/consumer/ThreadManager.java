package com.hooniegit.consumer;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;

public class ThreadManager {
	
	// Measure Total Threads
	public static void MeasureThreads() {
		ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
		
		while (true) {
            int activeThreadCount = threadMXBean.getThreadCount();
            int daemonThreadCount = threadMXBean.getDaemonThreadCount();
            int peakThreadCount = threadMXBean.getPeakThreadCount();
            long totalStartedThreadCount = threadMXBean.getTotalStartedThreadCount();

            System.out.println(">>> 현재 활성화된 스레드 개수: " + activeThreadCount);
            System.out.println(">>> 데몬 스레드 개수: " + daemonThreadCount);
            System.out.println(">>> 최대 스레드 개수: " + peakThreadCount);
            System.out.println(">>> 총 시작된 스레드 개수: " + totalStartedThreadCount);
           
            try {
            	// Update Duration = 5 Seconds
                Thread.sleep(5000); // 5초마다 업데이트
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
		}
	}
}
