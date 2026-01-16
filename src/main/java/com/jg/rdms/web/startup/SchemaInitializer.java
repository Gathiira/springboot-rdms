package com.jg.rdms.web.startup;

import com.jg.rdms.db.core.Database;
import com.jg.rdms.db.sql.SqlParser;
import lombok.AllArgsConstructor;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;

@Component
@AllArgsConstructor
public class SchemaInitializer {

    private final Database db;

    @PostConstruct
    public void init() {
        db.execute(
                SqlParser.parseCreateTable("""
                CREATE TABLE users (
                  id INT PRIMARY KEY,
                  name TEXT UNIQUE
                )
            """)
        );
    }
}
