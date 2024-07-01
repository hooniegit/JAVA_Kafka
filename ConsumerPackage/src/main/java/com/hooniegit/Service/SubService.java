package com.hooniegit.Service;

import java.util.concurrent.Callable;

public class SubService implements Callable<String> {
    private final String parameter;

    public SubService(String parameter) {
        this.parameter = parameter;
    }

    @Override
    public String call() throws Exception {
        // 파라미터를 처리하는 작업 구현
        // 예를 들어, 단순히 파라미터를 반환하는 작업
        return "Processed: " + parameter;
    }
}
