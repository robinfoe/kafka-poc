package com.rfoe.sample.kafka.health;

import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

@Component
public class CustomHealthCheck implements HealthIndicator{

    private static final String SERVICE_NAME = "CUSTOM_MOCK";

    @Override
    public Health health() {

        if(!isServiceRunning()){
            return Health.down().withDetail(CustomHealthCheck.SERVICE_NAME, "Not Available").build();
        }

        return Health.up().withDetail(CustomHealthCheck.SERVICE_NAME, "Available").build();
    }


    private Boolean isServiceRunning() {
        return false;
    }
    
}
