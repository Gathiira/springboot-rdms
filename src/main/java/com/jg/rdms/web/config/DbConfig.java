package com.jg.rdms.web.config;

import com.jg.rdms.db.core.Database;
import com.jg.rdms.db.tx.TransactionManager;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.nio.file.*;

@Configuration
@EnableConfigurationProperties(RdbmsProperties.class)
public class DbConfig {

    @Bean
    public Database database(RdbmsProperties props) throws IOException {
        Files.createDirectories(Path.of(props.getDataDir()));
        return new Database();
    }

    @Bean
    public TransactionManager transactionManager() {
        return new TransactionManager();
    }
}
