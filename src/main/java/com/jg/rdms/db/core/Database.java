package com.jg.rdms.db.core;

import com.jg.rdms.db.sql.CreateTableCommand;

import java.util.*;

public class Database {

    private final Map<String, Table> tables = new HashMap<>();

    /* =========================
       Catalog access
       ========================= */

    public Table table(String name) {
        return tables.get(name);
    }

    public boolean tableExists(String name) {
        return tables.containsKey(name);
    }

    public Collection<Table> allTables() {
        return tables.values();
    }

    /* =========================
       Table creation (PURE)
       ========================= */

    /**
     * Creates a persistent table instance and registers it in memory.
     * MUST NOT write to the system catalog.
     */
    public void createPersistentTable(String name, List<Column> columns) {
        if (tables.containsKey(name)) {
            return;
        }
        tables.put(name, new PersistentTable(this, name, columns));
    }

    /* =========================
       DDL execution
       ========================= */

    public void execute(CreateTableCommand cmd) {
        // 1. Create table instance
        createPersistentTable(cmd.tableName(), cmd.columns());

        // 2. Persist schema metadata
        persistSchema(cmd.tableName(), cmd.columns());
    }

    /* =========================
       Schema persistence
       ========================= */

    public void persistSchema(String tableName, List<Column> columns) {
        if (SystemTables.TABLES.equals(tableName)) {
            return; // bootstrap rule
        }

        Table catalog = table(SystemTables.TABLES);
        if (catalog == null) {
            throw new IllegalStateException("System catalog not initialized");
        }

        for (Column c : columns) {
            Map<String, Object> row = new HashMap<>();
            row.put("table_name", tableName);
            row.put("column_name", c.name());
            row.put("data_type", c.type().name());
            row.put("primary_key", c.primary() ? 1 : 0);
            row.put("unique_key", c.unique() ? 1 : 0);

            // 🔑 FOREIGN KEY METADATA
            row.put("ref_table", c.referencesTable());
            row.put("ref_column", c.referencesColumn());

            // system operation → no transaction
            catalog.insert(null, row);
        }
    }

    /* =========================
       Schema recovery
       ========================= */

    /**
     * Rebuilds table definitions from the system catalog.
     * MUST NOT write back to the catalog.
     */
    public void loadSchemaFromCatalog() {
        Table catalog = table(SystemTables.TABLES);
        if (catalog == null) {
            return; // first startup
        }

        Map<String, List<Column>> schemas = new HashMap<>();

        for (Map<String, Object> row : catalog.selectAll()) {
            String tableName = (String) row.get("table_name");

            schemas
                    .computeIfAbsent(tableName, k -> new ArrayList<>())
                    .add(new Column(
                            (String) row.get("column_name"),
                            DataType.valueOf((String) row.get("data_type")),
                            (Integer) row.get("primary_key") == 1,
                            (Integer) row.get("unique_key") == 1,
                            (String) row.get("ref_table"),
                            (String) row.get("ref_column")
                    ));
        }

        for (var entry : schemas.entrySet()) {
            createPersistentTable(entry.getKey(), entry.getValue());
        }
    }

    /* =========================
       DROP TABLE
       ========================= */

    public void dropTable(String name) {
        if (SystemTables.TABLES.equals(name)) {
            throw new IllegalStateException("Cannot drop system catalog");
        }

        Table table = tables.remove(name);
        if (table == null) {
            return;
        }

        if (table instanceof PersistentTable pt) {
            pt.deleteFiles();
        }

        // remove schema metadata
        Table catalog = tables.get(SystemTables.TABLES);
        if (catalog != null) {
            catalog.deleteWhere("table_name", name);
        }
    }

    public void bootstrapCatalog() {
        if (!tableExists(SystemTables.TABLES)) {
            createPersistentTable(
                    SystemTables.TABLES,
                    SystemTables.tablesSchema()
            );
        }
    }

    public void init() {

        // 1️⃣ system catalog
        bootstrapCatalog();

        // 2️⃣ load catalog rows ONLY
        PersistentTable catalog = (PersistentTable) table(SystemTables.TABLES);         // 🔑
        catalog.loadFromDisk();

        // 3️⃣ create EMPTY table definitions
        loadSchemaFromCatalog();      // 🚫 must NOT load data

        // 4️⃣ now load data exactly once
        for (Table t : allTables()) {
            if (t instanceof PersistentTable pt && pt != catalog) {
                pt.loadFromDisk();
            }
        }
    }


}
