package com.batch.poc.infrastructure.batch;

import com.batch.poc.domain.model.Customer;
import com.batch.poc.infrastructure.CustomerMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jspecify.annotations.Nullable;
import org.springframework.batch.infrastructure.item.ItemProcessor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class CustomerProcessor implements ItemProcessor<Customer, String> {

    private final CustomerMapper customerMapper;

    @Override
    public @Nullable String process(Customer customer) throws Exception {
        log.info("Transcription du customer d'index : {}", customer.getIndex());
        return customerMapper.toSqlRequest(customer);
    }
}
