package com.ibm.enpo.processor.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @author bishoybasily
 * @since 2020-08-24
 */

@Setter
@Getter
@ConfigurationProperties(prefix = "processor")
public class ProcessorProperties {

    private Archive archive;
    private Integer threadPoolSize;
    private String redirectUri;

    @Setter
    @Getter
    public static class Archive {

        private String password;
        private String targetFileName;

    }

}
