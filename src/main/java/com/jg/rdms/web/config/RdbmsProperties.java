package com.jg.rdms.web.config;


import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Setter
@Getter
@ConfigurationProperties(prefix = "rdbms")
public class RdbmsProperties {
    private String dataDir;
}