package com.jg.rdms.db.sql;

import java.util.List;

public record InsertCommand(
        String table,
        List<String> columns,
        List<Object> values
) {}
