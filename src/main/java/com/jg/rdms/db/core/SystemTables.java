package com.jg.rdms.db.core;

import java.util.List;

public final class SystemTables {

    public static final String TABLES = "__tables__";

    public static List<Column> tablesSchema() {
        return List.of(
                new Column(
                        "table_name",
                        DataType.TEXT,
                        false,
                        false,
                        null,
                        null
                ),
                new Column(
                        "column_name",
                        DataType.TEXT,
                        false,
                        false,
                        null,
                        null
                ),
                new Column(
                        "data_type",
                        DataType.TEXT,
                        false,
                        false,
                        null,
                        null
                ),
                new Column(
                        "primary_key",
                        DataType.INT,
                        false,
                        false,
                        null,
                        null
                ),
                new Column(
                        "unique_key",
                        DataType.INT,
                        false,
                        false,
                        null,
                        null
                ),
                // 🔑 FOREIGN KEY METADATA
                new Column(
                        "ref_table",
                        DataType.TEXT,
                        false,
                        false,
                        null,
                        null
                ),
                new Column(
                        "ref_column",
                        DataType.TEXT,
                        false,
                        false,
                        null,
                        null
                )
        );
    }

    private SystemTables() {
    }
}
