package com.batch.poc.infrastructure.batch;

import com.batch.poc.infrastructure.CustomerPartioner;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.partition.Partitioner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor
public class CsvPartitionerConfig {

    private final CustomerPartioner customerPartioner;

    @Bean
    public Partitioner csvPartitioner() {
        return gridSize -> customerPartioner.getPartitions(1_999_999L);
    }


}
