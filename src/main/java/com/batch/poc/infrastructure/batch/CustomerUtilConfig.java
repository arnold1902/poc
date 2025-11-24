package com.batch.poc.infrastructure.batch;

import com.batch.poc.infrastructure.CustomerMapper;
import com.batch.poc.infrastructure.CustomerPartioner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class CustomerUtilConfig {

    @Bean
    CustomerMapper getMapper() {
        return new CustomerMapper();
    }

    @Bean
    CustomerPartioner getPartioner() {
        return new CustomerPartioner();
    }
}
