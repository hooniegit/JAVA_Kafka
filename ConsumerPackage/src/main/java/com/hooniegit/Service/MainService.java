package com.hooniegit.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class MainService {
    private final ExecutorService executorService;

    public MainService(int numberOfThreads) {
        this.executorService = Executors.newFixedThreadPool(numberOfThreads);
    }

    public List<String> executeSubServices(List<String> parameters) throws InterruptedException, ExecutionException {
        List<Future<String>> futures = new ArrayList<>();
        
        for (String parameter : parameters) {
            SubService subService = new SubService(parameter);
            Future<String> future = executorService.submit(subService);
            futures.add(future);
        }

        List<String> results = new ArrayList<>();
        for (Future<String> future : futures) {
            results.add(future.get());
        }

        return results;
    }

    public void shutdown() {
        executorService.shutdown();
    }

    public static void main(String[] args) {
        List<String> parameters = List.of("param1", "param2", "param3", "param4");
        MainService mainService = new MainService(4);

        try {
            List<String> results = mainService.executeSubServices(parameters);
            for (String result : results) {
                System.out.println(result);
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        } finally {
            mainService.shutdown();
        }
    }
}
