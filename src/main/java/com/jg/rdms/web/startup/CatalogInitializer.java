package com.jg.rdms.web.startup;


import com.jg.rdms.db.core.Database;
import jakarta.annotation.PostConstruct;
import lombok.AllArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@AllArgsConstructor
public class CatalogInitializer {
    private final Database db;

    @PostConstruct
    public void initCatalog() {
        db.bootstrapCatalog();
    }
}
