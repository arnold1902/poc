package com.batch.poc.infrastructure;

import org.springframework.batch.infrastructure.item.ExecutionContext;

import java.util.HashMap;
import java.util.Map;

public class CustomerPartioner {

    private static final int PARTITIONSIZE = 8;

    public Map<String, ExecutionContext> getPartitions(long fileSize) {
        Map<String, ExecutionContext> result = new HashMap<>();
        long chunk = Math.floorDiv(fileSize, PARTITIONSIZE);
        long remainder = Math.floorMod(fileSize, PARTITIONSIZE);
        long start = 0L;
        for (int i = 0; i < PARTITIONSIZE; i++) {
            long end = start + chunk;
            if (i == 0) {
                end += remainder;
            }
            ExecutionContext executionContext = new ExecutionContext();
            executionContext.putLong("start", start);
            executionContext.putLong("end", end);
            executionContext.putString("partitionName", "partition " + i);
            result.put("partition " + i, executionContext);
            start = end;
        }
        return result;
    }
}
