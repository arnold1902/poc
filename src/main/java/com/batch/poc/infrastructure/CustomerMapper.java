package com.batch.poc.infrastructure;

import org.springframework.batch.infrastructure.item.file.LineMapper;

import com.batch.poc.domain.model.Customer;
import org.springframework.stereotype.Component;

public class CustomerMapper implements LineMapper<Customer>{

    @Override
    public Customer mapLine(String line, int lineNumber) throws Exception {
        String[] fields = line.split(",");
        Customer customer = new Customer();
        customer.setIndex(Long.parseLong(fields[0]));
        customer.setCustomerId(fields[1]);
        customer.setFirstName(fields[2]);
        customer.setLastName(fields[3]);
        customer.setCompany(fields[4]);
        customer.setCity(fields[5]);
        customer.setCountry(fields[6]);
        customer.setPhone1(fields[7]);
        customer.setPhone2(fields[8]);
        customer.setEmail(fields[9]);
        customer.setSubscriptionDate(fields[10]);
        customer.setWebsite(fields[11]);
        return customer;
    }

    public String toSqlRequest(Customer customer){
        StringBuilder sqlRequestBuilder = new StringBuilder();
        sqlRequestBuilder
                .append("INSERT INTO customers ")
                .append("(index, customer_id, first_name, last_name, company, ")
                .append("city, country, phone1, phone2, ")
                .append("email, subscription_date, website) ")
                .append("VALUES (")
                .append(customer.getIndex()).append(", '")
                .append(customer.getCustomerId()).append("', '")
                .append(customer.getFirstName()).append("', '")
                .append(customer.getLastName()).append("', '")
                .append(customer.getCompany()).append("', '")
                .append(customer.getCity()).append("', '")
                .append(customer.getCountry()).append("', '")
                .append(customer.getPhone1()).append("', '")
                .append(customer.getPhone2()).append("', '")
                .append(customer.getEmail()).append("', '")
                .append(customer.getSubscriptionDate()).append("', '")
                .append(customer.getWebsite()).append("');");
        return sqlRequestBuilder.toString();
    }
}
