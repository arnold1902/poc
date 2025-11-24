package com.batch.poc.infrastructure.config;

import com.batch.poc.domain.model.Customer;
import com.batch.poc.infrastructure.CustomerMapper;
import com.batch.poc.infrastructure.batch.CustomerProcessor;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.partition.Partitioner;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.item.file.FlatFileItemReader;
import org.springframework.batch.infrastructure.item.file.FlatFileItemWriter;
import org.springframework.batch.infrastructure.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.infrastructure.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
@RequiredArgsConstructor
@EnableBatchProcessing
public class BatchConfig {

    private final CustomerMapper customerMapper;
    private final CustomerProcessor customerProcessor;
    private final JobRepository jobRepository;
    private final Partitioner csvPartitioner;

    @StepScope
    @Bean
    public FlatFileItemReader<Customer> getReader(
            @Value("#{stepExecutionContext['start']}") Long start,
            @Value("#{stepExecutionContext['end']}") Long end
    ){
        int linesToSkip = Math.toIntExact(start == 0 ? 1 : start);
        return new FlatFileItemReaderBuilder<Customer>()
                .name("csvCustomerItemReader")
                .lineMapper(customerMapper)
                .linesToSkip(linesToSkip)
                .maxItemCount((int)(end - start + 1))
                .strict(true)
                .resource(new org.springframework.core.io.ClassPathResource("files/customers-2000000.csv"))
                .build();
    }


    @StepScope
    @Bean
    public FlatFileItemWriter<String> getWriter(@Value("#{stepExecutionContext['partitionName']}") String partitionName){
        return new FlatFileItemWriterBuilder<String>()
                .name("sqlCustomerItemWriter")
                .resource(new FileSystemResource("C:/Users/afots/Desktop/insert-customers-" + partitionName + ".sql"))
                .lineAggregator(item -> item)
                .encoding("UTF-8")
                .append(false)
                .build();
    }

    @Bean
    public Step slaveStep(FlatFileItemReader<Customer> reader, FlatFileItemWriter<String> writer) {
        return new StepBuilder("csvToSqlSlaveStep", jobRepository)
                .<Customer, String>chunk(1000)
                .reader(reader)
                .processor(customerProcessor)
                .writer(writer)
                .build();
    }

    @Bean
        public TaskExecutor partitionTaskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(8); // 8 partitions
        executor.setMaxPoolSize(8);
        executor.setQueueCapacity(0);
        executor.setThreadNamePrefix("partition-thread-");
        executor.initialize();
        return executor;
    }

    @Bean
    public Step masterStep(Step slaveStep) {
        return new StepBuilder("csvToSqlMasterStep", jobRepository)
                .partitioner(slaveStep.getName(), csvPartitioner)
                .taskExecutor(partitionTaskExecutor())
                .step(slaveStep)
                .build();
    }

    @Bean
    public Job csvToSqlJob(Step masterStep, JobRepository jobRepository) {
        return new JobBuilder("csvToSqlJob", jobRepository)
                .start(masterStep)
                .build();
    }

}
