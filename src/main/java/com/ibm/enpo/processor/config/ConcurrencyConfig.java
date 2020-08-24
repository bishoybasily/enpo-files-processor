package com.ibm.enpo.processor.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author bishoybasily
 * @since 2020-08-24
 */
@Configuration
public class ConcurrencyConfig {

    @Bean
    public ExecutorService executorService(ProcessorProperties props) {
        return Executors.newFixedThreadPool(props.getThreadPoolSize());
    }

}
