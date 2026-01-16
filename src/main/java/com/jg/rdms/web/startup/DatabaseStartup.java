package com.jg.rdms.web.startup;


import com.jg.rdms.db.core.Database;
import lombok.AllArgsConstructor;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.annotation.Configuration;

@Configuration
@AllArgsConstructor
public class DatabaseStartup  implements ApplicationRunner{
    private final Database db;

    @Override
    public void run(ApplicationArguments args) {
        // 1. Load schema from catalog
        db.init();
        System.out.println("Database startup completed");
    }
}