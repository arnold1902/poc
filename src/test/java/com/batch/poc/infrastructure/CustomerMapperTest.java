package com.batch.poc.infrastructure;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

import com.batch.poc.domain.model.Customer;

class CustomerMapperTest {

    private CustomerMapper customerMapper;
    
    public final String csvLines = "3,CUST003,Alice,Johnson,Tech Inc,Paris,France,01-42-86-00-00,01-42-86-00-01,alice.johnson@example.fr,2023-03-10,www.tech.fr";


    @BeforeEach
    void setUp() {
        customerMapper = new CustomerMapper();
    }
    
    @Test
    void testMapLines() throws Exception {
        Customer customer = customerMapper.mapLine(csvLines, 0);
        assertThat(customer).isNotNull();
        assertThat(customer.getIndex()).isEqualTo(Long.parseLong("3"));
        assertThat(customer.getCustomerId()).isEqualTo("CUST003");
        assertThat(customer.getFirstName()).isEqualTo("Alice");
        assertThat(customer.getLastName()).isEqualTo("Johnson");
        assertThat(customer.getCompany()).isEqualTo("Tech Inc");
        assertThat(customer.getCity()).isEqualTo("Paris");
        assertThat(customer.getCountry()).isEqualTo("France");
        assertThat(customer.getPhone1()).isEqualTo("01-42-86-00-00");
        assertThat(customer.getPhone2()).isEqualTo("01-42-86-00-01");
        assertThat(customer.getEmail()).isEqualTo("alice.johnson@example.fr");
        assertThat(customer.getSubscriptionDate()).isEqualTo("2023-03-10");
        assertThat(customer.getWebsite()).isEqualTo("www.tech.fr");
    }

    @Test
    void testToSqlRequest(){
        Customer customer = getCustomer();
        String sqlResult = customerMapper.toSqlRequest(customer);
        assertThat(sqlResult).isEqualTo("INSERT INTO customers (index, customer_id, first_name, last_name, company, city, country, phone1, phone2, email, subscription_date, website) VALUES (3, 'CUST003', 'Alice', 'Johnson', 'Tech Inc', 'Paris', 'France', '01-42-86-00-00', '01-42-86-00-01', 'alice.johnson@example.fr', '2023-03-10', 'www.tech.fr');");
    }

    private static Customer getCustomer() {
        Customer customer = new Customer();
        customer.setIndex(Long.parseLong("3"));
        customer.setCustomerId("CUST003");
        customer.setFirstName("Alice");
        customer.setLastName("Johnson");
        customer.setCompany("Tech Inc");
        customer.setCity("Paris");
        customer.setCountry("France");
        customer.setPhone1("01-42-86-00-00");
        customer.setPhone2("01-42-86-00-01");
        customer.setEmail("alice.johnson@example.fr");
        customer.setSubscriptionDate("2023-03-10");
        customer.setWebsite("www.tech.fr");
        return customer;
    }

}
