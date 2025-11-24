package com.batch.poc.infrastructure;

import org.junit.jupiter.api.Test;
import org.springframework.batch.infrastructure.item.ExecutionContext;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class CustomerPartionerTest {

    private CustomerPartioner customerPartioner = new CustomerPartioner();

    @Test
    void getPartitions() {
        Map<String, ExecutionContext> partions = customerPartioner.getPartitions(100L);
        assertThat(partions)
                .isNotNull()
                .isNotEmpty()
                .hasSize(8);

        assertThat(partions.get("partition 0"))
                .isNotNull();
        assertThat(partions.get("partition 0").get("start"))
                .isNotNull()
                .isEqualTo(0L);
        assertThat(partions.get("partition 0").get("end"))
                .isNotNull()
                .isEqualTo(16L);

        assertThat(partions.get("partition 1"))
                .isNotNull();
        assertThat(partions.get("partition 1").get("start"))
                .isNotNull()
                .isEqualTo(16L);
        assertThat(partions.get("partition 1").get("end"))
                .isNotNull()
                .isEqualTo(28L);

        assertThat(partions.get("partition 2"))
                .isNotNull();
        assertThat(partions.get("partition 2").get("start"))
                .isNotNull()
                .isEqualTo(28L);
        assertThat(partions.get("partition 2").get("end"))
                .isNotNull()
                .isEqualTo(40L);

        assertThat(partions.get("partition 3"))
                .isNotNull();
        assertThat(partions.get("partition 3").get("start"))
                .isNotNull()
                .isEqualTo(40L);
        assertThat(partions.get("partition 3").get("end"))
                .isNotNull()
                .isEqualTo(52L);

        assertThat(partions.get("partition 4"))
                .isNotNull();
        assertThat(partions.get("partition 4").get("start"))
                .isNotNull()
                .isEqualTo(52L);
        assertThat(partions.get("partition 4").get("end"))
                .isNotNull()
                .isEqualTo(64L);

        assertThat(partions.get("partition 5"))
                .isNotNull();
        assertThat(partions.get("partition 5").get("start"))
                .isNotNull()
                .isEqualTo(64L);
        assertThat(partions.get("partition 5").get("end"))
                .isNotNull()
                .isEqualTo(76L);

        assertThat(partions.get("partition 6"))
                .isNotNull();
        assertThat(partions.get("partition 6").get("start"))
                .isNotNull()
                .isEqualTo(76L);
        assertThat(partions.get("partition 6").get("end"))
                .isNotNull()
                .isEqualTo(88L);

        assertThat(partions.get("partition 7"))
                .isNotNull();
        assertThat(partions.get("partition 7").get("start"))
                .isNotNull()
                .isEqualTo(88L);
        assertThat(partions.get("partition 7").get("end"))
                .isNotNull()
                .isEqualTo(100L);
    }
}